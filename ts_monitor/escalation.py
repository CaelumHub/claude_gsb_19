"""
Alert Escalation Engine
- Repeat-trigger escalation: when the same metric+rule keeps firing within a
  short window, the alert severity is automatically raised (e.g. warning -> critical).
- Timeout escalation: when an alert stays unhandled (active, not acknowledged
  or resolved) longer than a configured duration, an escalation notification
  is sent (and severity is optionally raised).
- Configurable per rule via the rule's "escalation" block.
"""

import time
import threading
from typing import Dict, Any, Optional, Tuple, List

# Severity ordering (higher = more severe)
SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}

# Defaults used when a rule has no explicit escalation config
DEFAULT_REPEAT_WINDOW = 300      # count triggers within the last 5 minutes
DEFAULT_REPEAT_THRESHOLD = 3     # 3 triggers in that window -> escalate
DEFAULT_TIMEOUT = 0              # 0 disables timeout escalation
DEFAULT_TARGET_SEVERITY = "critical"
DEFAULT_NOTIFY = True
SWEEP_INTERVAL = 5              # timeout sweep cadence (seconds)


def normalize_escalation_config(rule: Dict) -> Dict:
    """Extract and validate the escalation config from a rule.

    Expected rule["escalation"] shape:
    {
        "enabled": true,
        "repeat_window": 300,          # seconds; window for counting repeats
        "repeat_threshold": 3,         # triggers within window -> escalate
        "repeat_target": "critical",   # severity to raise to on repeats
        "timeout": 600,                # unhandled seconds -> timeout escalation (0 = off)
        "timeout_target": "critical",  # severity to raise to on timeout
        "notify": true,                # send notification on escalation
        "channels": ["system"]         # notification channels
    }
    """
    raw = rule.get("escalation") or {}

    def _num(key, default):
        try:
            v = raw.get(key, default)
            return float(v) if v not in (None, "") else default
        except (TypeError, ValueError):
            return default

    def _severity(key, default):
        v = raw.get(key, default)
        return v if v in SEVERITY_ORDER else default

    return {
        "enabled": bool(raw.get("enabled", False)),
        "repeat_window": max(0, _num("repeat_window", DEFAULT_REPEAT_WINDOW)),
        "repeat_threshold": max(2, int(_num("repeat_threshold", DEFAULT_REPEAT_THRESHOLD))),
        "repeat_target": _severity("repeat_target", DEFAULT_TARGET_SEVERITY),
        "timeout": max(0, _num("timeout", DEFAULT_TIMEOUT)),
        "timeout_target": _severity("timeout_target", DEFAULT_TARGET_SEVERITY),
        "notify": bool(raw.get("notify", DEFAULT_NOTIFY)),
        "channels": raw.get("channels") or ["system"],
    }


class EscalationManager:
    """Applies repeat/timeout escalation rules to alerts."""

    def __init__(self, storage, detector=None):
        self.storage = storage
        self._sweep_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    # ---- Repeat-trigger path ----

    def process_anomaly(self, alert: Dict, rule: Dict) -> Tuple[Dict, str]:
        """Handle a newly detected anomaly against escalation rules.

        Returns (alert, action) where action is one of:
          "created"   - a brand new open alert
          "duplicate" - folded into an existing open alert (dedup window)
          "escalated" - folded in and severity was raised due to repeats
        """
        with self._lock:
            metric = alert.get("metric", "")
            rule_id = rule.get("id", "")
            now = float(alert.get("timestamp", time.time()))
            config = normalize_escalation_config(rule)

            existing = self.storage.find_open_alert(metric, rule_id, alert.get("tags"))
            if existing is not None:
                return self._handle_repeat(existing, alert, config, now)

            # Seed new alert with repeat-window bookkeeping
            alert["trigger_count"] = 1
            alert["first_triggered_at"] = now
            alert["last_triggered_at"] = now
            alert["trigger_times"] = [now]
            alert["original_severity"] = alert.get("severity", "warning")
            alert["escalation_level"] = 0
            alert["escalation_history"] = []
            saved = self.storage.add_alert(alert)
            return saved, "created"

    def _handle_repeat(self, existing: Dict, incoming: Dict,
                       config: Dict, now: float) -> Tuple[Dict, str]:
        """Fold a new anomaly into an open alert and maybe escalate."""
        window = config["repeat_window"] if config["enabled"] else DEFAULT_REPEAT_WINDOW

        updated = self.storage.record_alert_trigger(
            existing["id"], now,
            value=incoming.get("value"),
            score=incoming.get("score"),
        )

        # Drop triggers that fell outside the repeat window
        times = [t for t in updated.setdefault("trigger_times", []) if now - t <= window]
        if len(times) != len(updated["trigger_times"]):
            updated["trigger_times"] = times
            self.storage._save_json(self.storage.alerts_file, self.storage.alerts)

        if not config["enabled"]:
            # Plain deduplication behaviour for rules without escalation
            return updated, "duplicate"

        # Only unacknowledged (active) alerts auto-escalate; acknowledging
        # an alert means a human has taken ownership.
        if updated.get("status") != "active":
            return updated, "duplicate"

        if len(times) >= config["repeat_threshold"]:
            target = config["repeat_target"]
            if SEVERITY_ORDER.get(target, 1) > SEVERITY_ORDER.get(updated.get("severity"), 1):
                entry = self.storage.escalate_alert(
                    updated["id"], target, reason="repeat", timestamp=now,
                    detail=f"{len(times)} 次触发 / {int(window)} 秒窗口",
                )
                if entry:
                    self._notify(updated, entry, config, rule_id=updated.get("rule_id"))
                    return self.storage.get_alert(updated["id"]), "escalated"
            # Already at/above target severity: trigger_count keeps recording
            # repeats, but no per-trigger history entry is written to avoid
            # unbounded growth.

        return updated, "duplicate"

    # ---- Timeout path ----

    def sweep_timeouts(self, now: Optional[float] = None) -> List[Dict]:
        """Escalate alerts that have stayed unhandled past their timeout.

        Returns the list of escalation history entries produced.
        """
        now = now if now is not None else time.time()
        escalated = []
        rules_by_id = {r.get("id"): r for r in self.storage.get_rules()}

        # Snapshot open alerts so mutations below don't disturb iteration
        for alert in list(self.storage.get_open_alerts()):
            if alert.get("status") != "active":
                continue  # acknowledged = handled
            rule = rules_by_id.get(alert.get("rule_id"))
            if rule is None:
                continue
            config = normalize_escalation_config(rule)
            if not config["enabled"] or config["timeout"] <= 0:
                continue

            level = int(alert.get("escalation_level", 0))
            history = alert.get("escalation_history", [])
            timeout_count = sum(
                1 for h in history
                if h.get("type") == "escalated" and h.get("reason") == "timeout"
            )
            # Each level waits one more timeout interval:
            # level 0 -> at timeout, level 1 -> at 2*timeout, ...
            deadline = float(alert.get("first_triggered_at",
                                       alert.get("timestamp", now))) + (timeout_count + 1) * config["timeout"]
            if now < deadline:
                continue

            target = config["timeout_target"]
            if SEVERITY_ORDER.get(target, 1) <= SEVERITY_ORDER.get(alert.get("severity"), 1):
                # Already at/above target: send a repeat reminder notification,
                # but at most once per timeout interval.
                if config["notify"] and self._reminder_due(alert, config["timeout"], now):
                    entry = {
                        "type": "timeout_notify",
                        "reason": "timeout",
                        "detail": f"告警持续 {int(config['timeout'])} 秒未处理，重复通知",
                        "timestamp": now,
                    }
                    self.storage.append_escalation_entry(alert["id"], entry)
                    self._notify(alert, entry, config, rule_id=alert.get("rule_id"))
                    escalated.append(entry)
                continue

            entry = self.storage.escalate_alert(
                alert["id"], target, reason="timeout", timestamp=now,
                detail=f"告警持续超过 {int(config['timeout'])} 秒未处理",
            )
            if entry:
                self._notify(alert, entry, config, rule_id=alert.get("rule_id"))
                escalated.append(entry)

        return escalated

    def _reminder_due(self, alert: Dict, timeout: float, now: float) -> bool:
        last = 0
        for h in alert.get("escalation_history", []):
            if h.get("reason") == "timeout":
                last = max(last, float(h.get("timestamp", 0)))
        return (now - last) >= timeout

    # ---- Notifications ----

    def _notify(self, alert: Dict, entry: Dict, config: Dict,
                rule_id: Optional[str] = None):
        """Record an escalation notification (system channel + optional webhook)."""
        if not config.get("notify", True):
            return

        reason_text = "重复触发" if entry.get("reason") == "repeat" else "超时未处理"
        notification = {
            "alert_id": alert.get("id"),
            "rule_id": rule_id or alert.get("rule_id"),
            "rule_name": alert.get("rule_name"),
            "metric": alert.get("metric"),
            "severity": alert.get("severity"),
            "reason": entry.get("reason"),
            "title": f"[告警升级] {alert.get('metric')} → {alert.get('severity')}",
            "message": (
                f"{alert.get('metric')} 告警因{reason_text}已升级为 "
                f"{alert.get('severity')}（{entry.get('detail', '')}）"
            ),
            "channels": config.get("channels", ["system"]),
            "timestamp": entry.get("timestamp", time.time()),
            "status": "sent",
        }

        # Best-effort external webhook delivery (system channel always recorded)
        webhook = config.get("webhook_url")
        if webhook and "webhook" in notification["channels"]:
            notification["status"] = "sent" if self._send_webhook(webhook, notification) else "failed"

        self.storage.add_notification(notification)

    def _send_webhook(self, url: str, payload: Dict) -> bool:
        """Fire a simple JSON POST webhook without third-party dependencies."""
        import json
        import urllib.request
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                return 200 <= resp.status < 300
        except Exception as e:
            print(f"Escalation webhook failed ({url}): {e}")
            return False

    # ---- Background sweep lifecycle ----

    def start(self):
        """Start the background timeout-sweep thread."""
        if self._sweep_thread is not None:
            return
        self._stop_event.clear()
        self._sweep_thread = threading.Thread(
            target=self._sweep_loop, name="escalation-sweep", daemon=True
        )
        self._sweep_thread.start()

    def stop(self):
        self._stop_event.set()

    def _sweep_loop(self):
        while not self._stop_event.wait(SWEEP_INTERVAL):
            try:
                self.sweep_timeouts()
            except Exception as e:
                print(f"Escalation sweep error: {e}")
