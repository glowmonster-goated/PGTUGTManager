from __future__ import annotations

import asyncio
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from .config import Config
from .constants import TicketSection
from .utils import iso_now


class JsonStore:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.lock = asyncio.Lock()
        self.paths = {
            "blocks": config.data_dir / "blocks.json",
            "counters": config.data_dir / "counters.json",
            "punishments": config.data_dir / "punishments.json",
            "rules": config.data_dir / "rules.json",
            "settings": config.data_dir / "settings.json",
            "staff_stats": config.data_dir / "staff_stats.json",
            "tickets": config.data_dir / "tickets.json",
        }
        self.state: dict[str, Any] = {}

    async def initialize(self) -> None:
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self.config.transcript_dir.mkdir(parents=True, exist_ok=True)
        defaults = {
            "blocks": {"users": {}},
            "counters": {
                "appeal": 0,
                "management": 0,
                "pgt": 0,
                "ticket_internal": 0,
                "ugt": 0,
            },
            "punishments": {"items": {}},
            "rules": {"rules": []},
            "settings": {"panel_message_id": None},
            "staff_stats": {"staff": {}},
            "tickets": {"tickets": {}},
        }
        async with self.lock:
            for name, default in defaults.items():
                path = self.paths[name]
                if not path.exists():
                    await asyncio.to_thread(self._atomic_write, path, default)
                loaded = await asyncio.to_thread(self._read_json, path)
                self.state[name] = self._merge_defaults(default, loaded)

            self.state["counters"]["pgt"] = max(
                self.state["counters"]["pgt"],
                self.config.pgt_counter_start,
            )
            self.state["counters"]["ugt"] = max(
                self.state["counters"]["ugt"],
                self.config.ugt_counter_start,
            )
            self.state["counters"]["appeal"] = max(
                self.state["counters"]["appeal"],
                self.config.appeal_counter_start,
            )
            self.state["counters"]["management"] = max(
                self.state["counters"]["management"],
                self.config.management_counter_start,
            )
            await self._save_locked("counters")

    def _merge_defaults(self, default: Any, loaded: Any) -> Any:
        if isinstance(default, dict) and isinstance(loaded, dict):
            result = deepcopy(default)
            for key, value in loaded.items():
                if key in result:
                    result[key] = self._merge_defaults(result[key], value)
                else:
                    result[key] = value
            return result
        return loaded

    def _read_json(self, path: Path) -> Any:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _atomic_write(self, path: Path, payload: Any) -> None:
        temp_path = path.with_suffix(path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(temp_path, path)

    async def _save_locked(self, name: str) -> None:
        await asyncio.to_thread(self._atomic_write, self.paths[name], self.state[name])

    async def save_all(self) -> None:
        async with self.lock:
            for name in self.paths:
                await self._save_locked(name)

    async def next_ticket_identity(self, section: TicketSection) -> tuple[str, int]:
        async with self.lock:
            self.state["counters"]["ticket_internal"] += 1
            self.state["counters"][section.value] += 1
            internal_id = f"ticket-{self.state['counters']['ticket_internal']}"
            display_number = self.state["counters"][section.value]
            await self._save_locked("counters")
            return internal_id, display_number

    async def create_ticket(self, ticket: dict[str, Any]) -> None:
        async with self.lock:
            self.state["tickets"]["tickets"][ticket["ticket_id"]] = deepcopy(ticket)
            await self._save_locked("tickets")

    async def get_ticket(self, ticket_id: str) -> dict[str, Any] | None:
        async with self.lock:
            ticket = self.state["tickets"]["tickets"].get(ticket_id)
            return deepcopy(ticket) if ticket else None

    async def get_ticket_by_channel(self, channel_id: int) -> dict[str, Any] | None:
        async with self.lock:
            for ticket in self.state["tickets"]["tickets"].values():
                if ticket.get("channel_id") == channel_id and ticket.get("state") != "deleted":
                    return deepcopy(ticket)
            return None

    async def list_tickets(self) -> list[dict[str, Any]]:
        async with self.lock:
            return deepcopy(list(self.state["tickets"]["tickets"].values()))

    async def update_ticket(
        self,
        ticket_id: str,
        updater: Callable[[dict[str, Any]], dict[str, Any] | None],
    ) -> dict[str, Any]:
        async with self.lock:
            current = deepcopy(self.state["tickets"]["tickets"][ticket_id])
            updated = updater(current)
            if updated is None:
                raise RuntimeError(f"Ticket update for {ticket_id} returned None")
            self.state["tickets"]["tickets"][ticket_id] = updated
            await self._save_locked("tickets")
            return deepcopy(updated)

    async def find_open_ticket_for_owner(
        self,
        owner_id: int,
        section: TicketSection,
    ) -> dict[str, Any] | None:
        async with self.lock:
            for ticket in self.state["tickets"]["tickets"].values():
                if (
                    ticket.get("owner_id") == owner_id
                    and ticket.get("section") == section.value
                    and ticket.get("state") == "open"
                ):
                    return deepcopy(ticket)
            return None

    async def get_panel_message_id(self) -> int | None:
        async with self.lock:
            value = self.state["settings"].get("panel_message_id")
            return int(value) if value else None

    async def set_panel_message_id(self, message_id: int) -> None:
        async with self.lock:
            self.state["settings"]["panel_message_id"] = int(message_id)
            await self._save_locked("settings")

    async def list_rules(self) -> list[dict[str, Any]]:
        async with self.lock:
            return deepcopy(self.state["rules"].get("rules", []))

    async def get_rule(self, rule_id: str) -> dict[str, Any] | None:
        async with self.lock:
            for rule in self.state["rules"].get("rules", []):
                if rule.get("id") == rule_id:
                    return deepcopy(rule)
            return None

    async def get_block(self, user_id: int) -> dict[str, Any] | None:
        async with self.lock:
            block = self.state["blocks"]["users"].get(str(user_id))
            return deepcopy(block) if block else None

    async def set_block(self, user_id: int, payload: dict[str, Any]) -> None:
        async with self.lock:
            self.state["blocks"]["users"][str(user_id)] = deepcopy(payload)
            await self._save_locked("blocks")

    async def clear_block(self, user_id: int) -> None:
        async with self.lock:
            self.state["blocks"]["users"].pop(str(user_id), None)
            await self._save_locked("blocks")

    async def add_punishment(self, punishment: dict[str, Any]) -> None:
        async with self.lock:
            self.state["punishments"]["items"][punishment["id"]] = deepcopy(punishment)
            await self._save_locked("punishments")

    async def update_punishment(
        self,
        punishment_id: str,
        updater: Callable[[dict[str, Any]], dict[str, Any] | None],
    ) -> dict[str, Any]:
        async with self.lock:
            current = deepcopy(self.state["punishments"]["items"][punishment_id])
            updated = updater(current)
            if updated is None:
                raise RuntimeError(f"Punishment update for {punishment_id} returned None")
            self.state["punishments"]["items"][punishment_id] = updated
            await self._save_locked("punishments")
            return deepcopy(updated)

    async def list_punishments(self) -> list[dict[str, Any]]:
        async with self.lock:
            return deepcopy(list(self.state["punishments"]["items"].values()))

    async def record_staff_message(self, staff_id: int, ticket_id: str) -> None:
        await self._update_staff_stat(staff_id, ticket_id, "spoken_tickets")

    async def record_staff_close(self, staff_id: int, ticket_id: str) -> None:
        await self._update_staff_stat(staff_id, ticket_id, "closed_tickets")
        await self._update_staff_stat(staff_id, ticket_id, "handled_tickets")

    async def record_staff_action(self, staff_id: int, ticket_id: str) -> None:
        await self._update_staff_stat(staff_id, ticket_id, "handled_tickets")

    async def _update_staff_stat(self, staff_id: int, ticket_id: str, bucket: str) -> None:
        async with self.lock:
            staff = self.state["staff_stats"]["staff"].setdefault(
                str(staff_id),
                {
                    "handled_tickets": [],
                    "last_updated_at": iso_now(),
                    "spoken_tickets": [],
                    "closed_tickets": [],
                },
            )
            if ticket_id not in staff[bucket]:
                staff[bucket].append(ticket_id)
            staff["last_updated_at"] = iso_now()
            await self._save_locked("staff_stats")

    async def get_staff_stats(self) -> dict[str, Any]:
        async with self.lock:
            return deepcopy(self.state["staff_stats"]["staff"])
