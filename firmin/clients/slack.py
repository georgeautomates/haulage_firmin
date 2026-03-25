from __future__ import annotations
import os
import urllib.request
import json

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

STATUS_EMOJI = {
    "GREEN": ":large_green_circle:",
    "YELLOW": ":large_yellow_circle:",
    "RED": ":red_circle:",
    "ERROR": ":x:",
    "SKIPPED": ":white_circle:",
}


class SlackClient:
    def __init__(self, webhook_url: str | None = None):
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")

    def _post(self, payload: dict) -> bool:
        if not self.webhook_url:
            logger.debug("No SLACK_WEBHOOK_URL set — skipping notification")
            return False
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self.webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 200
        except Exception as e:
            logger.warning("Slack notification failed: %s", e)
            return False

    def post_batch_summary(
        self,
        email_subject: str,
        total_jobs: int,
        written: int,
        skipped: int,
        errors: int,
        orders: list[dict],  # each: {job_number, status, composite_score, collection_point, delivery_point, price}
    ) -> bool:
        green  = sum(1 for o in orders if o.get("status") == "GREEN")
        yellow = sum(1 for o in orders if o.get("status") == "YELLOW")
        red    = sum(1 for o in orders if o.get("status") == "RED")
        err    = sum(1 for o in orders if o.get("status") == "ERROR")

        header = (
            f":incoming_envelope: *New DS Smith booking batch processed*\n"
            f">Subject: _{email_subject}_"
        )

        summary = (
            f"*{total_jobs} jobs* found — "
            f"{STATUS_EMOJI['GREEN']} {green} GREEN  "
            f"{STATUS_EMOJI['YELLOW']} {yellow} YELLOW  "
            f"{STATUS_EMOJI['RED']} {red} RED"
            + (f"  {STATUS_EMOJI['ERROR']} {err} ERROR" if err else "")
            + (f"  :white_circle: {skipped} skipped (dedup)" if skipped else "")
        )

        # Per-job lines (only written orders)
        job_lines = []
        for o in orders:
            if o.get("status") in ("SKIPPED",):
                continue
            emoji = STATUS_EMOJI.get(o.get("status", ""), ":white_circle:")
            line = (
                f"{emoji} `{o['job_number']}`  "
                f"{o.get('collection_point', '—')} → {o.get('delivery_point', '—')}  "
                f"*{o.get('price', '—')}*  "
                f"_(score: {o.get('composite_score', '—')})_"
            )
            job_lines.append(line)

        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": header}},
            {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
        ]

        if job_lines:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(job_lines)},
            })

        if errors:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":warning: *{errors} job(s) failed to write to sheet* — check logs"},
            })

        return self._post({"blocks": blocks})

    def post_comparison_report(
        self,
        total_matched: int,
        full_match: int,
        partial: int,
        no_match: int,
        only_actual: int,
        only_verify: int,
        field_stats: dict[str, dict],  # {field_name: {match: int, total: int}}
        spreadsheet_url: str = "",
    ) -> bool:
        pct = full_match / max(total_matched, 1) * 100

        if pct >= 80:
            health_emoji = ":large_green_circle:"
        elif pct >= 60:
            health_emoji = ":large_yellow_circle:"
        else:
            health_emoji = ":red_circle:"

        header = f"{health_emoji} *Firmin vs Proteo — Comparison Report*"

        summary = (
            f"*{total_matched}* matched jobs\n"
            f"Full match: *{full_match}* ({pct:.1f}%)  |  "
            f"Partial: *{partial}*  |  No match: *{no_match}*\n"
            + (f":information_source: {only_actual} in Firmin only  |  {only_verify} in Proteo only" if (only_actual or only_verify) else "")
        ).strip()

        field_lines = []
        for field, s in field_stats.items():
            field_pct = s["match"] / max(s["total"], 1) * 100
            bar = ":large_green_circle:" if field_pct >= 90 else ":large_yellow_circle:" if field_pct >= 70 else ":red_circle:"
            field_lines.append(f"{bar} `{field}`: {s['match']}/{s['total']} ({field_pct:.0f}%)")

        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": header}},
            {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
        ]

        if field_lines:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Field breakdown:*\n" + "\n".join(field_lines)},
            })

        if spreadsheet_url:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":bar_chart: <{spreadsheet_url}|View full comparison in Sheets>"},
            })

        return self._post({"blocks": blocks})
