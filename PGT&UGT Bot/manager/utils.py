from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone


MENTION_RE = re.compile(r"[<@!>]")
DURATION_RE = re.compile(r"(?P<value>\d+)\s*(?P<unit>mo|[smhdwy])", re.IGNORECASE)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utcnow().isoformat()


def sanitize_user_id(raw_value: str) -> int:
    cleaned = MENTION_RE.sub("", raw_value.strip())
    if not cleaned.isdigit():
        raise ValueError("User ID must be a Discord user ID or mention.")
    return int(cleaned)


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    return cleaned.strip("-")


def parse_duration(raw_value: str) -> tuple[int | None, str]:
    lowered = raw_value.strip().lower()
    if lowered in {"perm", "permanent", "forever"}:
        return None, "Permanent"

    total = 0
    for match in DURATION_RE.finditer(lowered):
        amount = int(match.group("value"))
        unit = match.group("unit").lower()
        multiplier = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
            "w": 604800,
            "mo": 2592000,
            "y": 31536000,
        }[unit]
        total += amount * multiplier

    if total <= 0:
        raise ValueError("Use a duration like 1m, 1h, 1d, 1w, 1mo, 1y, or permanent.")

    return total, human_duration(total)


def human_duration(total_seconds: int | None) -> str:
    if total_seconds is None:
        return "Permanent"
    remainder = total_seconds
    parts: list[str] = []
    units = (
        ("y", 31536000),
        ("mo", 2592000),
        ("w", 604800),
        ("d", 86400),
        ("h", 3600),
        ("m", 60),
    )
    for label, size in units:
        amount, remainder = divmod(remainder, size)
        if amount:
            parts.append(f"{amount}{label}")
    seconds = remainder
    if seconds and not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts) or "0s"
