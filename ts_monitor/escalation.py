"""
Alert Escalation Engine
-----------------------
Two escalation mechanisms, both configurable per detection rule:

1. Repeat-trigger escalation
   When the same metric+rule keeps firing within a rolling time window,
   the open incident is automatically promoted to a higher severity
   (e.g. warning -> critical).

2. Timeout escalation
   When an active incident stays unacknowledged / unhandled past a
   configured duration, its severity is raised (first timeout stage)
   and escalation notifications are sent (subsequent stages).

Severity ladder: info < warning < critical.

All state changes are written to the alert's ``history`` list so the UI
can render a full escalation timeline. Notifications are persisted via
the storage layer and optionally delivered to a rule-level webhook.
"""

import json
import time
import threading
import urllib.request
import urllib.error
from typing import Dict, Any, Optional, List

SEVERITY_ORDER = ["info", "warning", "critical"]
SEVERITY_LABELS = {"info": "信息", "warning": "警告", "critical": "严重"}


def severity_rank(sev: str) -> int:
    try:
        return SEVERITY_ORDER.index(sev)
    except ValueError:
        return 1


def next_severity(sev: str) -> str:
    idx = min(severity_rank(sev) + 1, len(SEVERITY_ORDER) - 1)
    return SEVERITY_ORDER[idx]


# Defaults used when a rule has no escalation config (demo-friendly)
DEFAULT_ESCALATION = {
    "enabled": True,
    # Repeat-trigger escalation: fire >= repeat_count times within
    # repeat_window seconds -> promote to repeat_severity
    "repeat_enabled": True,
    "repeat_count": 3,
    "repeat_window": 300,
    "repeat_severity": "critical",
    # Timeout escalation: unhandled for N seconds -> raise / notify
    "timeout_enabled": True,
    "timeout_stages": [
        {"after_seconds": 60, "action": "escalate", "severity": "critical"},
        {"after_seconds": 300, "action": "notify", "message": "告警持续 5 分钟未处理，已再次通知值班人员"},
    ],
    # Optional outgoing webhook for escalation notifications
    "webhook_url": "",
}


def normalize_escalation_config(rule: Dict) -> Dict:
    """
    Build an effective escalation config for a rule, filling defaults and
    tolerating legacy/partial configs.
    """
    cfg = dict(DEFAULT_ESCALATION)
    raw = rule.get("escalation")
    if isinstance(raw, dict):
        cfg.update(raw)

    # Legacy flat fields: escalation_enabled / escalate_after_seconds
    if not isinstance(raw, dict) and "escalation_enabled" in rule:
        cfg["enabled"] = bool(rule.get("escalation_enabled"))
    if not isinstance(raw, dict) and rule.get("escalate_after_seconds"):
        try:
            cfg["timeout_stages"] = [
                {"after_seconds": int(rule["escalate_after_seconds"]),
                 "action": "escalate", "severity": "critical"},
            ]
        except (TypeError, ValueError):
            pass

    # Validate / sanitize timeout stages
    stages = []
    for stage in cfg.get("timeout_stages") or []:
        try:
            after = int(stage.get("after_seconds", 0))
        except (TypeError, ValueError):
            continue
        if after <= 0:
            continue
        action = stage.get("action", "notify")
        entry = {"after_seconds": after, "action": action}
        if action == "escalate":
            sev = stage.get("severity", "critical")
            entry["severity"] = sev if sev in SEVERITY_ORDER else "critical"
        else:
            entry["message"] = stage.get("message", "告警超时未处理，请尽快关注")
        stages.append(entry)
    stages.sort(key=lambda s: s["after_seconds"])
    cfg["timeout_stages"] = stages

    try:
        cfg["repeat_count"] = max(2, int(cfg.get("repeat_count", 3)))
    except (TypeError, ValueError):
        cfg["repeat_count"] = 3
    try:
        cfg["repeat_window"] = max(10, int(cfg.get("repeat_window", 300)))
    except (TypeError, ValueError):
        cfg["repeat_window"] = 300
    if cfg.get("repeat_severity") not in SEVERITY_ORDER:
        cfg["repeat_severity"] = "critical"
    return cfg


class EscalationManager:
    """Applies escalation rules and dispatches notifications."""

    def __init__(self, storage, check_interval: float = 5.0):
        self.storage = storage
        self.check_interval = check_interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Runtime counters
        self.repeat_escalations = 0
        self.timeout_escalations = 0
        self.notifications_sent = 0

    # ---- Lifecycle ----

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="escalation-checker", daemon=True
        )
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def _run_loop(self):
        while not self._stop_event.wait(self.check_interval):
            try:
                self.check_timeouts()
            except Exception as e:  # never let the checker die
                print(f"[escalation] timeout check error: {e}")

    # ---- Rule lookup ----

    def _rule_map(self) -> Dict[str, Dict]:
        return {r.get("id"): r for r in self.storage.get_rules()}

    # ---- Repeat-trigger escalation ----

    def process_anomaly_alert(self, alert: Dict) -> Dict:
        """
        Hook called after storage.add_alert() for every anomaly event.
        Merged (repeated) incidents may trigger repeat escalation once.
        """
        if not alert.get("merged"):
            return alert

        rule = self._rule_map().get(alert.get("rule_id"), {})
        cfg = normalize_escalation_config(rule)
        if not cfg["enabled"] or not cfg["repeat_enabled"]:
            return alert

        now = float(alert.get("last_time", alert.get("timestamp", time.time())))
        window_start = now - cfg["repeat_window"]
        triggers = [t for t in alert.get("trigger_times", [alert["timestamp"]])
                    if t >= window_start]
        repeat_count = len(triggers)

        already = alert.get("repeat_escalated", False)
        target_rank = severity_rank(cfg["repeat_severity"])
        current_rank = severity_rank(alert.get("severity", "warning"))

        if not already and repeat_count >= cfg["repeat_count"] and target_rank > current_rank:
            updated = self.storage.update_alert(
                alert["id"],
                {"repeat_escalated": True},
                history_entry={
                    "type": "repeat_escalated",
                    "label": "重复触发升级",
                    "from_severity": alert.get("severity", "warning"),
                    "to_severity": cfg["repeat_severity"],
                    "detail": (f"{cfg['repeat_window']} 秒内重复触发 {repeat_count} 次，"
                               f"自动升级为{SEVERITY_LABELS[cfg['repeat_severity']]}"),
                },
            )
            if updated:
                alert = updated
                self.repeat_escalations += 1
                self._send_notification(
                    alert, cfg,
                    title="告警重复触发自动升级",
                    message=(f"指标 {alert['metric']} 在 {cfg['repeat_window']} 秒内重复触发 "
                             f"{repeat_count} 次，级别已升级为 "
                             f"{SEVERITY_LABELS[cfg['repeat_severity']]}"),
                    escalation_type="repeat",
                )

        # Record this merged trigger as a lightweight timeline marker
        self.storage.append_alert_history(alert["id"], {
            "type": "repeat",
            "label": "重复触发",
            "timestamp": now,
            "detail": f"第 {alert.get('repeat_count', repeat_count)} 次触发，"
                      f"当前值 {alert.get('last_value', '-')}",
        })
        return alert

    # ---- Timeout escalation ----

    def check_timeouts(self) -> List[Dict]:
        """
        Scan open incidents and fire timeout stages whose delay has
        elapsed. Only *active* (unacknowledged) incidents are escalated;
        acknowledging an incident pauses timeout escalation.
        """
        fired = []
        rule_map = self._rule_map()
        now = time.time()

        for alert in self.storage.get_open_alerts():
            if alert.get("status") != "active":
                continue

            rule = rule_map.get(alert.get("rule_id"), {})
            cfg = normalize_escalation_config(rule)
            if not cfg["enabled"] or not cfg["timeout_enabled"]:
                continue

            created = float(alert.get("timestamp", now))
            # Defensive: only consider stages against a valid creation time
            if created > now:
                created = now
            age = max(0.0, now - created)
            fired_stages = set(alert.get("timeout_stages_fired", []))

            for idx, stage in enumerate(cfg["timeout_stages"]):
                key = f"{stage['after_seconds']}:{stage['action']}"
                if key in fired_stages or age < stage["after_seconds"]:
                    continue

                if stage["action"] == "escalate":
                    target = stage.get("severity", "critical")
                    if severity_rank(target) <= severity_rank(alert.get("severity", "warning")):
                        # Nothing to promote; still notify at this stage once
                        self._fire_timeout_notify(
                            alert, cfg, stage,
                            f"告警持续 {stage['after_seconds']} 秒未处理",
                        )
                    else:
                        updated = self.storage.update_alert(
                            alert["id"],
                            {"timeout_escalated": True},
                            history_entry={
                                "type": "timeout_escalated",
                                "label": "超时升级",
                                "from_severity": alert.get("severity", "warning"),
                                "to_severity": target,
                                "detail": f"持续 {stage['after_seconds']} 秒未处理，"
                                          f"自动升级为{SEVERITY_LABELS[target]}并发送升级通知",
                            },
                        )
                        if updated:
                            alert = updated
                            self.timeout_escalations += 1
                            self._send_notification(
                                alert, cfg,
                                title="告警超时自动升级",
                                message=(f"指标 {alert['metric']} 的告警已持续 "
                                         f"{stage['after_seconds']} 秒未处理，级别升级为 "
                                         f"{SEVERITY_LABELS[target]}"),
                                escalation_type="timeout",
                            )
                else:
                    self._fire_timeout_notify(
                        alert, cfg, stage,
                        stage.get("message", f"告警持续 {stage['after_seconds']} 秒未处理"),
                    )

                # Persist that this stage has fired (idempotent across ticks)
                cur = self.storage.get_alert(alert["id"])
                if cur:
                    done = set(cur.get("timeout_stages_fired", []))
                    done.add(key)
                    self.storage.update_alert(alert["id"],
                                              {"timeout_stages_fired": sorted(done)})
                    alert = self.storage.get_alert(alert["id"]) or alert

                fired.append(alert)
        return fired

    def _fire_timeout_notify(self, alert: Dict, cfg: Dict, stage: Dict,
                             message: str):
        """Record a timeout notification stage (severity unchanged)."""
        self.storage.append_alert_history(alert["id"], {
            "type": "timeout_notified",
            "label": "超时通知",
            "detail": f"持续 {stage['after_seconds']} 秒未处理：{message}",
        }, bump_escalation=True)
        refreshed = self.storage.get_alert(alert["id"]) or alert
        self._send_notification(
            refreshed, cfg,
            title="告警超时升级通知",
            message=f"[{refreshed.get('metric')}] {message}",
            escalation_type="timeout",
        )

    # ---- Notifications ----

    def _send_notification(self, alert: Dict, cfg: Dict,
                           title: str, message: str,
                           escalation_type: str):
        """Persist a notification record, log it, and POST to webhook if set."""
        record = {
            "alert_id": alert.get("id"),
            "metric": alert.get("metric"),
            "rule_id": alert.get("rule_id"),
            "rule_name": alert.get("rule_name"),
            "severity": alert.get("severity"),
            "escalation_type": escalation_type,
            "title": title,
            "message": message,
            "channels": ["log"],
            "timestamp": time.time(),
        }

        webhook = cfg.get("webhook_url", "").strip()
        if webhook:
            ok = self._post_webhook(webhook, {
                "title": title,
                "message": message,
                "alert": {k: alert.get(k) for k in
                          ("id", "metric", "severity", "status", "value",
                           "repeat_count", "escalation_count", "timestamp")},
                "timestamp": record["timestamp"],
            })
            record["webhook_url"] = webhook
            record["webhook_ok"] = ok
            if ok:
                record["channels"].append("webhook")

        self.storage.add_notification(record)
        self.notifications_sent += 1
        print(f"[escalation] {title}: {message}")

    @staticmethod
    def _post_webhook(url: str, payload: Dict, timeout: float = 3.0) -> bool:
        """Best-effort JSON POST to a notification webhook."""
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return 200 <= resp.status < 300
        except (urllib.error.URLError, OSError, ValueError) as e:
            print(f"[escalation] webhook delivery failed: {e}")
            return False

    def get_stats(self) -> Dict:
        return {
            "repeat_escalations": self.repeat_escalations,
            "timeout_escalations": self.timeout_escalations,
            "notifications_sent": self.notifications_sent,
            "check_interval": self.check_interval,
        }
