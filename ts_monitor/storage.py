"""
Time-Series Storage Engine
- Hourly JSON shard files for time-series data
- Separate metadata and rules storage
- Cross-shard query with efficient merging
- Write-ahead buffer for high-throughput ingestion
"""

import json
import os
import time
import threading
import uuid
import copy
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Optional, Tuple

class TimeSeriesStorage:
    """Manages time-series data with hourly JSON shard files."""

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.ts_dir = os.path.join(data_dir, "timeseries")
        self.meta_file = os.path.join(data_dir, "metadata.json")
        self.rules_file = os.path.join(data_dir, "rules.json")
        self.alerts_file = os.path.join(data_dir, "alerts.json")

        os.makedirs(self.ts_dir, exist_ok=True)

        # Write buffer for high-throughput ingestion
        self._write_buffer: Dict[str, List[Dict]] = defaultdict(list)
        self._buffer_lock = threading.Lock()
        self._buffer_flush_interval = 2.0  # seconds
        self._last_flush = time.time()

        # In-memory cache for recent data (last 2 hours)
        self._cache: Dict[str, List[Dict]] = defaultdict(list)
        self._cache_lock = threading.Lock()
        self._max_cache_points = 50000

        # Load metadata and rules
        self.metadata = self._load_json(self.meta_file, {"sources": {}, "stats": {}})
        self.rules = self._load_json(self.rules_file, {"rules": []})
        self.alerts = self._load_json(
            self.alerts_file,
            {"alerts": [], "suppressed": {}, "notifications": []}
        )
        # Backward compatibility: ensure notifications list exists
        self.alerts.setdefault("notifications", [])

        # Guards all in-memory alert/rule mutations and JSON persistence
        self._alert_lock = threading.RLock()

    def _load_json(self, path: str, default: Any) -> Any:
        """Load JSON file with fallback to default."""
        try:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
        return default

    def _save_json(self, path: str, data: Any):
        """Atomically save JSON file."""
        tmp_path = path + ".tmp"
        try:
            with open(tmp_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp_path, path)
        except IOError as e:
            print(f"Error saving {path}: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _get_shard_path(self, metric: str, timestamp: float) -> str:
        """Get the hourly shard file path for a metric and timestamp."""
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        shard_key = dt.strftime("%Y%m%d_%H")
        safe_metric = metric.replace("/", "_").replace(".", "_").replace(" ", "_")
        return os.path.join(self.ts_dir, f"{safe_metric}_{shard_key}.json")

    def _get_shard_key(self, metric: str, timestamp: float) -> str:
        """Get the shard key for caching."""
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return f"{metric}_{dt.strftime('%Y%m%d_%H')}"

    def write(self, metric: str, timestamp: float, value: float,
              tags: Optional[Dict[str, str]] = None, source: str = "default"):
        """Write a single data point to the write buffer."""
        point = {
            "t": round(timestamp, 3),
            "v": value,
            "tags": tags or {},
            "src": source
        }

        with self._buffer_lock:
            self._write_buffer[metric].append(point)
            # Auto-flush if buffer is large enough
            if len(self._write_buffer[metric]) >= 1000 or \
               (time.time() - self._last_flush) > self._buffer_flush_interval:
                self._flush_buffer()

        # Update cache
        with self._cache_lock:
            self._cache[metric].append(point)
            # Trim cache if too large
            if len(self._cache[metric]) > self._max_cache_points:
                self._cache[metric] = self._cache[metric][-self._max_cache_points:]

    def write_batch(self, points: List[Dict[str, Any]]):
        """Write multiple data points efficiently."""
        with self._buffer_lock:
            for p in points:
                metric = p.get("metric", "unknown")
                point = {
                    "t": round(p.get("timestamp", time.time()), 3),
                    "v": p.get("value", 0),
                    "tags": p.get("tags", {}),
                    "src": p.get("source", "default")
                }
                self._write_buffer[metric].append(point)

                with self._cache_lock:
                    self._cache[metric].append(point)

            if any(len(v) >= 500 for v in self._write_buffer.values()):
                self._flush_buffer()

    def _flush_buffer(self):
        """Flush write buffer to shard files."""
        if not self._write_buffer:
            return

        shards_to_write: Dict[str, List[Dict]] = defaultdict(list)

        for metric, points in self._write_buffer.items():
            for point in points:
                shard_path = self._get_shard_path(metric, point["t"])
                shards_to_write[shard_path].append(point)

        for shard_path, points in shards_to_write.items():
            existing = []
            if os.path.exists(shard_path):
                try:
                    with open(shard_path, 'r') as f:
                        existing = json.load(f)
                except (json.JSONDecodeError, IOError):
                    existing = []

            existing.extend(points)
            # Sort by timestamp and deduplicate
            existing.sort(key=lambda x: x["t"])
            # Remove exact duplicates
            seen = set()
            unique = []
            for p in existing:
                key = (p["t"], p["v"])
                if key not in seen:
                    seen.add(key)
                    unique.append(p)
            existing = unique

            # Keep only last 10000 points per shard to prevent unbounded growth
            if len(existing) > 10000:
                existing = existing[-10000:]

            try:
                tmp_path = shard_path + ".tmp"
                with open(tmp_path, 'w') as f:
                    json.dump(existing, f)
                os.replace(tmp_path, shard_path)
            except IOError as e:
                print(f"Error writing shard {shard_path}: {e}")

        self._write_buffer.clear()
        self._last_flush = time.time()

    def force_flush(self):
        """Force flush all buffered data."""
        with self._buffer_lock:
            self._flush_buffer()

    def query(self, metric: str, start: float, end: float,
              tags: Optional[Dict[str, str]] = None,
              max_points: int = 10000) -> List[Dict]:
        """Query time-series data across shards."""
        self.force_flush()

        results = []

        # Determine which hourly shards to read
        start_dt = datetime.fromtimestamp(start, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end, tz=timezone.utc)

        current = start_dt.replace(minute=0, second=0, microsecond=0)
        while current <= end_dt + timedelta(hours=1):
            shard_path = self._get_shard_path(metric, current.timestamp())
            if os.path.exists(shard_path):
                try:
                    with open(shard_path, 'r') as f:
                        points = json.load(f)
                    # Filter by time range
                    filtered = [p for p in points if start <= p["t"] <= end]
                    if tags:
                        filtered = [p for p in filtered
                                   if all(p.get("tags", {}).get(k) == v for k, v in tags.items())]
                    results.extend(filtered)
                except (json.JSONDecodeError, IOError):
                    pass
            current += timedelta(hours=1)

        # Also check cache for very recent data
        with self._cache_lock:
            cache_points = self._cache.get(metric, [])
            cache_filtered = [p for p in cache_points if start <= p["t"] <= end]
            if tags:
                cache_filtered = [p for p in cache_filtered
                                 if all(p.get("tags", {}).get(k) == v for k, v in tags.items())]
            results.extend(cache_filtered)

        # Deduplicate and sort
        seen = set()
        unique = []
        for p in sorted(results, key=lambda x: x["t"]):
            key = (p["t"], p["v"])
            if key not in seen:
                seen.add(key)
                unique.append(p)

        # Downsample if too many points
        if len(unique) > max_points:
            step = len(unique) / max_points
            unique = [unique[int(i * step)] for i in range(max_points)]

        return unique

    def get_metrics(self) -> List[str]:
        """Get list of all available metrics."""
        metrics = set()
        # Scan shard files
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                if fname.endswith('.json'):
                    # Extract metric name (everything before the date part)
                    parts = fname.rsplit('_', 2)
                    if len(parts) >= 3:
                        metrics.add(parts[0])
        # Also include cached metrics
        with self._cache_lock:
            metrics.update(self._cache.keys())
        return sorted(metrics)

    def get_shard_info(self) -> List[Dict]:
        """Get information about shard files."""
        info = []
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                if fname.endswith('.json'):
                    fpath = os.path.join(self.ts_dir, fname)
                    stat = os.stat(fpath)
                    info.append({
                        "file": fname,
                        "size": stat.st_size,
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
        return sorted(info, key=lambda x: x["file"])

    # ---- Metadata (Sources) ----

    def get_sources(self) -> Dict:
        """Get all configured data sources."""
        return self.metadata.get("sources", {})

    def add_source(self, source_id: str, config: Dict) -> Dict:
        """Add or update a data source."""
        self.metadata["sources"][source_id] = {
            **config,
            "id": source_id,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        self._save_json(self.meta_file, self.metadata)
        return self.metadata["sources"][source_id]

    def delete_source(self, source_id: str) -> bool:
        """Delete a data source."""
        if source_id in self.metadata.get("sources", {}):
            del self.metadata["sources"][source_id]
            self._save_json(self.meta_file, self.metadata)
            return True
        return False

    # ---- Rules ----

    def get_rules(self) -> List[Dict]:
        """Get all anomaly detection rules."""
        return self.rules.get("rules", [])

    def add_rule(self, rule: Dict) -> Dict:
        """Add or update an anomaly detection rule."""
        with self._alert_lock:
            rule_id = rule.get("id", f"rule_{int(time.time()*1000)}")
            rule["id"] = rule_id
            rule["updated_at"] = datetime.now(timezone.utc).isoformat()

            # Update existing or add new
            existing = [r for r in self.rules["rules"] if r["id"] != rule_id]
            existing.append(rule)
            self.rules["rules"] = existing

            self._save_json(self.rules_file, self.rules)
            return copy.deepcopy(rule)

    def delete_rule(self, rule_id: str) -> bool:
        """Delete an anomaly detection rule."""
        with self._alert_lock:
            before = len(self.rules["rules"])
            self.rules["rules"] = [r for r in self.rules["rules"] if r["id"] != rule_id]
            if len(self.rules["rules"]) < before:
                self._save_json(self.rules_file, self.rules)
                return True
            return False

    # ---- Alerts (incident model with escalation history) ----

    def get_alerts(self, status: Optional[str] = None,
                   severity: Optional[str] = None,
                   limit: int = 200) -> List[Dict]:
        """Get alerts with optional filtering (returns copies)."""
        with self._alert_lock:
            alerts = copy.deepcopy(self.alerts.get("alerts", []))
        if status:
            alerts = [a for a in alerts if a.get("status") == status]
        if severity:
            alerts = [a for a in alerts if a.get("severity") == severity]
        return sorted(alerts, key=lambda x: x.get("last_time", x.get("timestamp", 0)),
                      reverse=True)[:limit]

    def get_open_alerts(self) -> List[Dict]:
        """Get active/acknowledged alerts (the still-open incidents)."""
        with self._alert_lock:
            return copy.deepcopy([
                a for a in self.alerts.get("alerts", [])
                if a.get("status") in ("active", "acknowledged")
            ])

    def get_alert(self, alert_id: str) -> Optional[Dict]:
        """Get a single alert by id (returns a copy)."""
        with self._alert_lock:
            for a in self.alerts.get("alerts", []):
                if a.get("id") == alert_id:
                    return copy.deepcopy(a)
        return None

    def add_alert(self, alert: Dict) -> Dict:
        """
        Add an anomaly event.

        If an open (active/acknowledged) alert with the same metric+rule
        exists, the event is merged into that incident: the repeat counter
        is incremented instead of creating a new alert. This replaces the
        old fixed 5-minute suppression with incident-based grouping which
        the escalation engine uses for repeat-trigger escalation.

        Returns a copy of the alert. When merged, the returned dict carries
        a transient ``merged=True`` flag.
        """
        with self._alert_lock:
            metric = alert.get("metric", "")
            rule_id = alert.get("rule_id", "")
            now = float(alert.get("timestamp", time.time()))

            # Merge into an existing open incident for the same metric+rule
            existing = None
            for a in self.alerts.get("alerts", []):
                if (a.get("metric") == metric and a.get("rule_id") == rule_id
                        and a.get("status") in ("active", "acknowledged")):
                    existing = a
                    break

            if existing is not None:
                existing["repeat_count"] = existing.get("repeat_count", 1) + 1
                existing["last_time"] = now
                existing["last_value"] = alert.get("value")
                if alert.get("score") is not None:
                    existing["last_score"] = alert.get("score")
                # Rolling list of trigger timestamps (used for repeat escalation)
                triggers = existing.setdefault(
                    "trigger_times", [existing.get("timestamp", now)]
                )
                triggers.append(now)
                # Only the recent window matters; keep the list bounded
                existing["trigger_times"] = triggers[-500:]
                self._save_json(self.alerts_file, self.alerts)
                result = copy.deepcopy(existing)
                result["merged"] = True
                return result

            # New incident
            alert_id = f"alert_{uuid.uuid4().hex[:12]}"
            alert["id"] = alert_id
            alert["timestamp"] = now
            alert["status"] = alert.get("status", "active")
            alert["repeat_count"] = 1
            alert["escalation_count"] = 0
            alert["trigger_times"] = [now]
            alert["history"] = [{
                "type": "created",
                "label": "告警触发",
                "timestamp": now,
                "from_severity": None,
                "to_severity": alert.get("severity", "warning"),
                "detail": f"检测算法 {alert.get('algorithm', '-')} 首次触发，"
                          f"当前值 {alert.get('value')}"
            }]

            self.alerts["alerts"].append(alert)
            # Keep only last 1000 alerts
            if len(self.alerts["alerts"]) > 1000:
                self.alerts["alerts"] = self.alerts["alerts"][-1000:]

            self._save_json(self.alerts_file, self.alerts)
            return copy.deepcopy(alert)

    def update_alert(self, alert_id: str, fields: Dict,
                     history_entry: Optional[Dict] = None) -> Optional[Dict]:
        """
        Update fields of an alert and optionally append an escalation
        history entry. Returns the updated alert copy, or None if missing.
        """
        with self._alert_lock:
            alert = None
            for a in self.alerts.get("alerts", []):
                if a.get("id") == alert_id:
                    alert = a
                    break
            if alert is None:
                return None

            alert.update(fields)
            if history_entry:
                entry = {"timestamp": time.time(), **history_entry}
                alert.setdefault("history", []).append(entry)
                alert["history"] = alert["history"][-100:]
                # Escalation entries actually change the alert's severity
                to_sev = entry.get("to_severity")
                if to_sev and entry.get("type") in (
                        "repeat_escalated", "timeout_escalated", "manual_escalated"):
                    alert["severity"] = to_sev
                alert["escalation_count"] = alert.get("escalation_count", 0) + 1
            self._save_json(self.alerts_file, self.alerts)
            return copy.deepcopy(alert)

    def append_alert_history(self, alert_id: str, entry: Dict,
                             bump_escalation: bool = False) -> Optional[Dict]:
        """Append a history entry (without changing severity)."""
        with self._alert_lock:
            alert = None
            for a in self.alerts.get("alerts", []):
                if a.get("id") == alert_id:
                    alert = a
                    break
            if alert is None:
                return None
            alert.setdefault("history", []).append(
                {"timestamp": time.time(), **entry}
            )
            alert["history"] = alert["history"][-100:]
            if bump_escalation:
                alert["escalation_count"] = alert.get("escalation_count", 0) + 1
            self._save_json(self.alerts_file, self.alerts)
            return copy.deepcopy(alert)

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        with self._alert_lock:
            for alert in self.alerts.get("alerts", []):
                if alert.get("id") == alert_id:
                    if alert.get("status") == "active":
                        alert["status"] = "acknowledged"
                        alert["acknowledged_at"] = time.time()
                        alert.setdefault("history", []).append({
                            "type": "acknowledged",
                            "label": "告警确认",
                            "timestamp": time.time(),
                            "detail": "运维人员已确认告警，超时升级暂停"
                        })
                        self._save_json(self.alerts_file, self.alerts)
                    return True
            return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        with self._alert_lock:
            for alert in self.alerts.get("alerts", []):
                if alert.get("id") == alert_id:
                    if alert.get("status") != "resolved":
                        alert["status"] = "resolved"
                        alert["resolved_at"] = time.time()
                        alert.setdefault("history", []).append({
                            "type": "resolved",
                            "label": "告警解决",
                            "timestamp": time.time(),
                            "detail": "告警已处理并关闭"
                        })
                        self._save_json(self.alerts_file, self.alerts)
                    return True
            return False

    # ---- Escalation notifications ----

    def add_notification(self, notification: Dict) -> Dict:
        """Persist an escalation notification record."""
        with self._alert_lock:
            notification.setdefault("id", f"ntf_{uuid.uuid4().hex[:12]}")
            notification.setdefault("timestamp", time.time())
            notifications = self.alerts.setdefault("notifications", [])
            notifications.append(notification)
            if len(notifications) > 500:
                self.alerts["notifications"] = notifications[-500:]
            self._save_json(self.alerts_file, self.alerts)
            return copy.deepcopy(notification)

    def get_notifications(self, limit: int = 100) -> List[Dict]:
        """Get recent escalation notifications (newest first)."""
        with self._alert_lock:
            notifications = copy.deepcopy(self.alerts.get("notifications", []))
        return sorted(notifications, key=lambda x: x.get("timestamp", 0),
                      reverse=True)[:limit]

    def cleanup_suppressed(self):
        """Clean up old suppression entries (legacy compatibility)."""
        with self._alert_lock:
            suppressed = self.alerts.get("suppressed", {})
            now = time.time()
            self.alerts["suppressed"] = {
                k: v for k, v in suppressed.items()
                if now - v < 600  # Keep 10 minutes of suppression history
            }
            self._save_json(self.alerts_file, self.alerts)

    def get_stats(self) -> Dict:
        """Get storage statistics."""
        total_size = 0
        shard_count = 0
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                if fname.endswith('.json'):
                    fpath = os.path.join(self.ts_dir, fname)
                    total_size += os.path.getsize(fpath)
                    shard_count += 1

        return {
            "shard_count": shard_count,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "metric_count": len(self.get_metrics()),
            "source_count": len(self.get_sources()),
            "rule_count": len(self.get_rules()),
            "alert_count": len(self.alerts.get("alerts", [])),
            "cache_size": sum(len(v) for v in self._cache.values())
        }