from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands, tasks

from .config import Config
from .constants import (
    CLOSE_BUTTON_ID,
    DELETE_BUTTON_ID,
    MANAGEMENT_CANCEL_ID,
    MANAGEMENT_CONFIRM_ID,
    PANEL_CUSTOM_IDS,
    StaffLevel,
    TicketSection,
    TicketState,
)
from .permissions import (
    get_staff_level,
    member_has_level,
    normalize_transcript_access,
    transcript_access_key_for_level,
)
from .punishments import PunishmentService
from .storage import JsonStore
from .tickets import TicketService
from .transcripts import TranscriptStore
from .utils import parse_duration, sanitize_user_id


class TicketPanelView(discord.ui.View):
    def __init__(self, bot: "ManagerBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Appeal",
        style=discord.ButtonStyle.secondary,
        custom_id=PANEL_CUSTOM_IDS[TicketSection.APPEAL],
    )
    async def appeal(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.bot.create_ticket_from_button(interaction, TicketSection.APPEAL)

    @discord.ui.button(
        label="UGT Support",
        style=discord.ButtonStyle.success,
        custom_id=PANEL_CUSTOM_IDS[TicketSection.UGT],
    )
    async def ugt(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.bot.create_ticket_from_button(interaction, TicketSection.UGT)

    @discord.ui.button(
        label="PGT Support",
        style=discord.ButtonStyle.primary,
        custom_id=PANEL_CUSTOM_IDS[TicketSection.PGT],
    )
    async def pgt(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.bot.create_ticket_from_button(interaction, TicketSection.PGT)

    @discord.ui.button(
        label="Management",
        style=discord.ButtonStyle.danger,
        custom_id=PANEL_CUSTOM_IDS[TicketSection.MANAGEMENT],
    )
    async def management(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        embed = self.bot.ticket_service.build_management_warning_embed()
        await interaction.response.send_message(
            embed=embed,
            view=ManagementWarningView(self.bot),
            ephemeral=True,
        )


class ManagementWarningView(discord.ui.View):
    def __init__(self, bot: "ManagerBot") -> None:
        super().__init__(timeout=180)
        self.bot = bot

    @discord.ui.button(label="Open Management Ticket", style=discord.ButtonStyle.danger, custom_id=MANAGEMENT_CONFIRM_ID)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.bot.create_ticket_from_button(interaction, TicketSection.MANAGEMENT)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, custom_id=MANAGEMENT_CANCEL_ID)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.edit_message(content="Management ticket creation cancelled.", embed=None, view=None)


class CloseTicketView(discord.ui.View):
    def __init__(self, bot: "ManagerBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.secondary, custom_id=CLOSE_BUTTON_ID)
    async def close(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.bot.handle_close_button(interaction)


class DeleteTicketView(discord.ui.View):
    def __init__(self, bot: "ManagerBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Delete Ticket", style=discord.ButtonStyle.danger, custom_id=DELETE_BUTTON_ID)
    async def delete(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.bot.handle_delete_button(interaction)


class PunishmentEditModal(discord.ui.Modal, title="Edit Punishment Preview"):
    def __init__(self, view: "PunishmentReviewView") -> None:
        super().__init__()
        self.review_view = view
        self.rule_id = discord.ui.TextInput(
            label="Rule ID",
            default=str(view.rule.get("id", "")),
            max_length=100,
        )
        self.extra_comments = discord.ui.TextInput(
            label="Extra Comments",
            default=view.extra_comments or "",
            required=False,
            style=discord.TextStyle.paragraph,
            max_length=1000,
        )
        self.add_item(self.rule_id)
        self.add_item(self.extra_comments)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.review_view.actor.id:
            await interaction.response.send_message(
                "Only the staff member who opened this preview can edit it.",
                ephemeral=True,
            )
            return
        selected_rule = await self.review_view.cog.bot.store.get_rule(self.rule_id.value.strip())
        if not selected_rule:
            await interaction.response.send_message("That rule ID was not found.", ephemeral=True)
            return
        if str(selected_rule.get("action", "ban")).lower() == "manual":
            await interaction.response.send_message(
                "That rule is marked as a staff action. Use `/manual-ban` instead.",
                ephemeral=True,
            )
            return
        selected_rule, escalation_note = await self.review_view.cog._apply_rule_escalation(
            self.review_view.user_id,
            selected_rule,
        )
        if str(selected_rule.get("action", "ban")).lower() == "manual":
            await interaction.response.send_message(
                "That rule escalates to a manual staff action. Use `/manual-ban` instead.",
                ephemeral=True,
            )
            return
        self.review_view.rule = selected_rule
        self.review_view.escalation_note = escalation_note
        self.review_view.extra_comments = self.extra_comments.value.strip() or None
        await self.review_view.refresh_preview()
        await interaction.response.send_message("Punishment preview updated.", ephemeral=True)


class PunishmentReviewView(discord.ui.View):
    def __init__(
        self,
        cog: "ManagerCog",
        command_interaction: discord.Interaction,
        ticket: dict[str, Any] | None,
        actor: discord.Member,
        user_id: int,
        rule: dict[str, Any],
        extra_comments: str | None,
        proof: dict[str, Any],
        source_context: dict[str, Any],
        escalation_note: str | None = None,
    ) -> None:
        super().__init__(timeout=900)
        self.cog = cog
        self.command_interaction = command_interaction
        self.ticket = ticket
        self.actor = actor
        self.user_id = user_id
        self.rule = rule
        self.extra_comments = extra_comments
        self.proof = proof
        self.source_context = source_context
        self.escalation_note = escalation_note

    async def refresh_preview(self) -> None:
        embed = await self.cog.build_punishment_review_embed(
            self.ticket,
            self.user_id,
            self.rule,
            self.extra_comments,
            self.proof,
            self.source_context,
            escalation_note=self.escalation_note,
        )
        await self.command_interaction.edit_original_response(embed=embed, view=self)

    async def _deny_if_other_user(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.actor.id:
            return False
        await interaction.response.send_message(
            "Only the staff member who opened this preview can use these buttons.",
            ephemeral=True,
        )
        return True

    @discord.ui.button(label="Confirm Punishment", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._deny_if_other_user(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        punishment = await self.cog.bot.punishment_service.punish_from_rule(
            self.ticket,
            self.actor,
            self.user_id,
            self.rule,
            self.extra_comments,
            self.proof,
            self.source_context,
        )
        embed = await self.cog.build_punishment_review_embed(
            self.ticket,
            self.user_id,
            self.rule,
            self.extra_comments,
            self.proof,
            self.source_context,
            punishment=punishment,
            escalation_note=self.escalation_note,
        )
        for item in self.children:
            item.disabled = True
        await self.command_interaction.edit_original_response(embed=embed, view=self)
        await interaction.followup.send(
            (
                f"Recorded `{punishment['action']}` for `{punishment['user_id']}` with `{punishment['rule_label']}`.\n"
                f"Duration: {punishment['duration_text']}"
                + (
                    f"\nBan success: {punishment.get('ban_success_count', 0)}/"
                    f"{len(punishment.get('ban_results', []))}"
                    if punishment["action"] == "ban"
                    else (
                        f"\nMute success: {punishment.get('mute_success_count', 0)}/"
                        f"{max(len(punishment.get('mute_results', [])), 1)}"
                        if punishment["action"] == "mute"
                        else ""
                    )
                )
            ),
            ephemeral=True,
        )

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary)
    async def edit(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._deny_if_other_user(interaction):
            return
        await interaction.response.send_modal(PunishmentEditModal(self))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._deny_if_other_user(interaction):
            return
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content="Punishment cancelled.",
            embed=None,
            view=self,
        )


class LiftPunishmentSelect(discord.ui.Select):
    def __init__(self, view: "UnbanReviewView") -> None:
        options: list[discord.SelectOption] = []
        for punishment in view.punishments[:25]:
            label = view.cog._format_lift_option_label(punishment)
            description = view.cog._format_lift_option_description(punishment)
            options.append(
                discord.SelectOption(
                    label=label[:100],
                    value=str(punishment["id"]),
                    description=description[:100],
                    default=punishment["id"] == view.selected_punishment_id,
                )
            )
        super().__init__(
            placeholder="Select the active punishment to lift",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.review_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        if await self.review_view._deny_if_other_user(interaction):
            return
        self.review_view.selected_punishment_id = self.values[0]
        for option in self.options:
            option.default = option.value == self.review_view.selected_punishment_id
        await interaction.response.defer()
        await self.review_view.refresh_preview()


class UnbanReviewView(discord.ui.View):
    def __init__(
        self,
        cog: "ManagerCog",
        command_interaction: discord.Interaction,
        actor: discord.Member,
        user_id: int,
        reason: str,
        punishments: list[dict[str, Any]],
    ) -> None:
        super().__init__(timeout=900)
        self.cog = cog
        self.command_interaction = command_interaction
        self.actor = actor
        self.user_id = user_id
        self.reason = reason
        self.punishments = punishments[:25]
        self.selected_punishment_id = self.punishments[0]["id"] if self.punishments else None
        self.result: dict[str, Any] | None = None
        if self.punishments:
            self.add_item(LiftPunishmentSelect(self))

    def selected_punishment(self) -> dict[str, Any] | None:
        if self.selected_punishment_id is None:
            return None
        for punishment in self.punishments:
            if punishment["id"] == self.selected_punishment_id:
                return punishment
        return None

    async def refresh_preview(self) -> None:
        embed = self.cog.build_unban_review_embed(
            self.user_id,
            self.reason,
            self.punishments,
            self.selected_punishment(),
            result=self.result,
        )
        await self.command_interaction.edit_original_response(embed=embed, view=self)

    async def _deny_if_other_user(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.actor.id:
            return False
        await interaction.response.send_message(
            "Only the staff member who opened this review can use these controls.",
            ephemeral=True,
        )
        return True

    @discord.ui.button(label="Confirm Lift", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._deny_if_other_user(interaction):
            return
        selected = self.selected_punishment()
        if selected is None:
            await interaction.response.send_message(
                "Select an active punishment first.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        try:
            self.result = await self.cog.bot.punishment_service.lift_selected_punishment(
                self.actor,
                selected["id"],
                self.reason,
            )
        except RuntimeError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return
        for item in self.children:
            item.disabled = True
        await self.refresh_preview()
        await interaction.followup.send(
            (
                f"Lifted `{self.result['action']}` for `{self.result['user_id']}`.\n"
                f"Success: {self.result.get('success_count', 0)}/"
                f"{len(self.result.get('results', []))}\n"
                f"DM Status: {self.result.get('dm_status', 'unknown')}"
            ),
            ephemeral=True,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._deny_if_other_user(interaction):
            return
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content="Punishment lift cancelled.",
            embed=None,
            view=self,
        )


class RevokeCaseHistorySelect(discord.ui.Select):
    def __init__(self, view: "RevokeCaseHistoryView") -> None:
        options: list[discord.SelectOption] = []
        for record in view.record_rows[:25]:
            options.append(
                discord.SelectOption(
                    label=str(record["select_label"])[:100],
                    value=str(record["select_value"]),
                    description=str(record["select_description"])[:100],
                    default=str(record["select_value"]) == view.selected_record_value,
                )
            )
        super().__init__(
            placeholder="Choose a case or punishment to revoke from this user's history",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.review_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        if await self.review_view._deny_if_invalid_user(interaction):
            return
        self.review_view.selected_record_value = self.values[0]
        for option in self.options:
            option.default = option.value == self.review_view.selected_record_value
        await interaction.response.defer()
        await self.review_view.refresh_preview()


class RevokeCaseHistoryView(discord.ui.View):
    def __init__(
        self,
        cog: "ManagerCog",
        actor: discord.Member,
        target_user_id: int,
        record_rows: list[dict[str, Any]],
    ) -> None:
        super().__init__(timeout=900)
        self.cog = cog
        self.actor = actor
        self.target_user_id = target_user_id
        self.record_rows = record_rows[:25]
        self.selected_record_value = str(self.record_rows[0]["select_value"]) if self.record_rows else None
        self.message: discord.InteractionMessage | None = None
        self.result_label: str | None = None
        if self.record_rows:
            self.add_item(RevokeCaseHistorySelect(self))

    def selected_record(self) -> dict[str, Any] | None:
        if self.selected_record_value is None:
            return None
        for record in self.record_rows:
            if str(record["select_value"]) == self.selected_record_value:
                return record
        return None

    def build_embed(self) -> discord.Embed:
        selected = self.selected_record()
        embed = discord.Embed(
            title="Revoke Case History",
            description=(
                f"User ID: `{self.target_user_id}`\n"
                "Pick a visible case or punishment below to remove it from this user's staff history views."
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(
            name="Records Available",
            value=str(len(self.record_rows)),
            inline=True,
        )
        embed.add_field(
            name="Action",
            value="Permanent hide from `/user-info` and staff dossier history",
            inline=True,
        )
        if selected:
            embed.add_field(
                name="Selected Record",
                value=str(selected["summary"])[:1024],
                inline=False,
            )
        if self.result_label:
            embed.add_field(
                name="Completed",
                value=f"Revoked `{self.result_label}` from this user's visible history.",
                inline=False,
            )
        return embed

    async def refresh_preview(self) -> None:
        if self.message is not None:
            await self.message.edit(embed=self.build_embed(), view=self)

    async def _deny_if_invalid_user(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor.id:
            await interaction.response.send_message(
                "Only the League Manager who opened this control can use it.",
                ephemeral=True,
            )
            return True
        if await self.cog._support_staff_level(interaction.user.id) < StaffLevel.LEAGUE_MANAGER:
            await interaction.response.send_message(
                "Only League Managers can revoke cases from user history.",
                ephemeral=True,
            )
            return True
        return False

    @discord.ui.button(label="Confirm Revoke", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._deny_if_invalid_user(interaction):
            return
        selected = self.selected_record()
        if selected is None:
            await interaction.response.send_message(
                "Select a case or punishment first.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        if selected["record_kind"] == "ticket":
            await self.cog._revoke_ticket_from_user_history(
                selected["record_id"],
                self.target_user_id,
                interaction.user,
            )
        else:
            await self.cog._revoke_punishment_from_user_history(
                selected["record_id"],
                self.target_user_id,
                interaction.user,
            )
        self.result_label = str(selected["result_label"])
        for item in self.children:
            item.disabled = True
        await self.refresh_preview()
        await interaction.followup.send(
            f"Revoked `{selected['result_label']}` from `{self.target_user_id}`'s visible history.",
            ephemeral=True,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._deny_if_invalid_user(interaction):
            return
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content="Case history revoke cancelled.",
            embed=None,
            view=self,
        )


class UserInfoActionsView(discord.ui.View):
    def __init__(
        self,
        cog: "ManagerCog",
        actor: discord.Member,
        target_user_id: int,
        record_rows: list[dict[str, Any]],
        *,
        allow_revoke: bool,
    ) -> None:
        super().__init__(timeout=900)
        self.cog = cog
        self.actor = actor
        self.target_user_id = target_user_id
        self.record_rows = record_rows
        self.revoke_history.disabled = not allow_revoke

    async def _deny_if_invalid_user(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.actor.id:
            await interaction.response.send_message(
                "Only the staff member who opened this user-info panel can use its controls.",
                ephemeral=True,
            )
            return True
        if await self.cog._support_staff_level(interaction.user.id) < StaffLevel.LEAGUE_MANAGER:
            await interaction.response.send_message(
                "Only League Managers can revoke cases from user history.",
                ephemeral=True,
            )
            return True
        return False

    @discord.ui.button(label="Revoke Case History", style=discord.ButtonStyle.danger)
    async def revoke_history(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._deny_if_invalid_user(interaction):
            return
        latest_payload = await self.cog._build_user_history_payload(self.target_user_id, interaction.user.id)
        record_rows = latest_payload["revoke_case_rows"]
        if not record_rows:
            await interaction.response.send_message(
                "There are no visible cases or punishments left to revoke from this user's history.",
                ephemeral=True,
            )
            return
        view = RevokeCaseHistoryView(
            self.cog,
            self.actor,
            self.target_user_id,
            record_rows,
        )
        embed = view.build_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()


class ManagerCog(commands.Cog):
    def __init__(self, bot: "ManagerBot") -> None:
        self.bot = bot
        self.video_extensions = {".avi", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".webm"}

    async def _ensure_support_guild(self, interaction: discord.Interaction) -> bool:
        if interaction.guild_id != self.bot.config.support_guild_id:
            await interaction.response.send_message(
                "This command only works in the support server.",
                ephemeral=True,
            )
            return False
        return True

    async def _require_level(
        self,
        interaction: discord.Interaction,
        level: StaffLevel,
    ) -> bool:
        if not await self._ensure_support_guild(interaction):
            return False
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not member_has_level(member, level, self.bot.config):
            await interaction.response.send_message("You do not have permission to use that command.", ephemeral=True)
            return False
        return True

    async def _require_ticket(
        self,
        interaction: discord.Interaction,
    ) -> tuple[dict[str, Any], discord.TextChannel] | tuple[None, None]:
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("Use this inside a ticket channel.", ephemeral=True)
            return None, None
        ticket = await self.bot.store.get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
            return None, None
        return ticket, interaction.channel

    async def _optional_ticket(self, interaction: discord.Interaction) -> dict[str, Any] | None:
        if not isinstance(interaction.channel, discord.TextChannel):
            return None
        return await self.bot.store.get_ticket_by_channel(interaction.channel.id)

    async def _require_punish_level(
        self,
        interaction: discord.Interaction,
        level: StaffLevel,
    ) -> bool:
        if interaction.guild_id not in self.bot.managed_guild_ids:
            await interaction.response.send_message(
                "This command only works in the support, PGT, or UGT servers.",
                ephemeral=True,
            )
            return False

        staff_level = await self._support_staff_level(interaction.user.id)
        if staff_level < level:
            await interaction.response.send_message(
                "You do not have permission to use that command.",
                ephemeral=True,
            )
            return False
        return True

    async def _support_staff_level(self, user_id: int) -> StaffLevel:
        support_member = await self._resolve_support_member(user_id)
        return get_staff_level(support_member, self.bot.config)

    async def _user_can_view_ticket_for_lookup(
        self,
        caller_id: int,
        caller_staff_level: StaffLevel,
        ticket: dict[str, Any],
    ) -> bool:
        access = normalize_transcript_access(TicketSection(ticket["section"]), ticket.get("transcript_access"))
        if caller_id == ticket["owner_id"] and access["owner"]:
            return True
        role_key = transcript_access_key_for_level(caller_staff_level)
        return bool(role_key and access.get(role_key, False))

    def _ticket_history_revoked_for_user(self, ticket: dict[str, Any], user_id: int) -> bool:
        for item in ticket.get("history_revocations", []):
            if isinstance(item, dict):
                value = item.get("user_id")
            else:
                value = item
            try:
                if int(value) == user_id:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    def _punishment_history_revoked_for_user(self, punishment: dict[str, Any], user_id: int) -> bool:
        for item in punishment.get("history_revocations", []):
            if isinstance(item, dict):
                value = item.get("user_id")
            else:
                value = item
            try:
                if int(value) == user_id:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    async def _build_user_history_payload(
        self,
        target_user_id: int,
        caller_id: int,
    ) -> dict[str, Any]:
        caller_staff_level = await self._support_staff_level(caller_id)
        block = await self.bot.store.get_block(target_user_id)
        tickets = await self.bot.store.list_tickets()
        ticket_map = {ticket["ticket_id"]: ticket for ticket in tickets}

        visible_ticket_rows: list[str] = []
        hidden_ticket_count = 0
        owner_ticket_count = 0
        participant_ticket_count = 0
        added_ticket_count = 0
        all_involved_ticket_ids: list[str] = []
        revoke_case_rows: list[dict[str, Any]] = []

        for ticket in tickets:
            if self._ticket_history_revoked_for_user(ticket, target_user_id):
                continue

            transcript = await self.bot.transcripts.get_transcript(ticket["ticket_id"])
            transcript_messages = (transcript or {}).get("messages", {})
            participant_ids = set((transcript or {}).get("participants", {}).keys())
            added_user_ids = {int(item) for item in ticket.get("added_user_ids", [])}
            is_owner = ticket.get("owner_id") == target_user_id
            is_participant = str(target_user_id) in participant_ids
            is_added_user = target_user_id in added_user_ids
            if not is_owner and not is_participant and not is_added_user:
                continue

            all_involved_ticket_ids.append(ticket["ticket_id"])
            owner_ticket_count += int(is_owner)
            participant_ticket_count += int(is_participant and not is_owner)
            added_ticket_count += int(is_added_user and not is_owner and not is_participant)

            if not await self._user_can_view_ticket_for_lookup(caller_id, caller_staff_level, ticket):
                hidden_ticket_count += 1
                continue

            message_count = sum(
                1 for message in transcript_messages.values() if int(message.get("author_id", 0) or 0) == target_user_id
            )
            role_bits: list[str] = []
            if is_owner:
                role_bits.append("Owner")
            if is_participant:
                role_bits.append("Participant")
            if is_added_user and not is_participant:
                role_bits.append("Added User")
            role_text = " / ".join(role_bits) if role_bits else "Involved"
            visible_ticket_rows.append(
                (
                    f"- {ticket['ticket_id']} | {ticket['channel_name']} | {ticket['section']} | {ticket['state']}\n"
                    f"  Role: {role_text}\n"
                    f"  Messages by user: {message_count}\n"
                    f"  Transcript: {ticket.get('transcript_url', 'Unavailable')}"
                )
            )
            revoke_case_rows.append(
                {
                    "record_id": ticket["ticket_id"],
                    "record_kind": "ticket",
                    "result_label": ticket["ticket_id"],
                    "select_description": (
                        f"{role_text} | {TicketSection(ticket['section']).label} | "
                        f"{str(ticket['state']).replace('_', ' ').title()}"
                    ),
                    "select_label": f"{ticket['ticket_id']} · {ticket['channel_name']}",
                    "select_value": f"ticket:{ticket['ticket_id']}",
                    "sort_key": str(ticket.get("created_at", "")),
                    "summary": (
                        f"`{ticket['ticket_id']}`\n"
                        f"{ticket['channel_name']}\n"
                        f"{role_text} | {TicketSection(ticket['section']).label} | "
                        f"{str(ticket['state']).replace('_', ' ').title()}"
                    ),
                }
            )

        punishments = await self.bot.store.list_punishments()
        matching_punishments = [item for item in punishments if int(item.get("user_id", 0) or 0) == target_user_id]
        matching_punishments.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)

        visible_punishment_rows: list[str] = []
        hidden_punishment_count = 0
        for punishment in matching_punishments:
            if self._punishment_history_revoked_for_user(punishment, target_user_id):
                continue
            linked_ticket = ticket_map.get(str(punishment.get("ticket_id"))) if punishment.get("ticket_id") else None
            if linked_ticket and self._ticket_history_revoked_for_user(linked_ticket, target_user_id):
                continue
            if linked_ticket and not await self._user_can_view_ticket_for_lookup(
                caller_id,
                caller_staff_level,
                linked_ticket,
            ):
                hidden_punishment_count += 1
                continue

            proof = punishment.get("proof") or {}
            proof_url = str(proof.get("url", "")).strip() or "No proof recorded"
            proof_name = str(proof.get("filename", "")).strip() or "Unknown file"
            location = (
                linked_ticket.get("transcript_url", "")
                if linked_ticket
                else f"{punishment.get('context_guild_name', 'Unknown Guild')} / #{punishment.get('context_channel_name', 'unknown-channel')}"
            )
            visible_punishment_rows.append(
                (
                    f"- {punishment.get('created_at', 'Unknown')} | {str(punishment.get('action', 'punishment')).title()} | "
                    f"{punishment.get('duration_text', 'Unknown')} | {punishment.get('status', 'recorded')}\n"
                    f"  Reason: {punishment.get('reason', 'No reason recorded')}\n"
                    f"  Rule: {punishment.get('rule_label') or punishment.get('rule_id') or 'Manual/Unknown'}\n"
                    f"  Proof: {proof_name} - {proof_url}\n"
                    f"  Context: {location}"
                )
            )
            action_label = str(punishment.get("action", "punishment")).title()
            rule_label = str(punishment.get("rule_label") or punishment.get("rule_id") or "Manual / Unknown")
            revoke_case_rows.append(
                {
                    "record_id": punishment["id"],
                    "record_kind": "punishment",
                    "result_label": rule_label,
                    "select_description": (
                        f"{action_label} | {punishment.get('duration_text', 'Unknown')} | "
                        f"{str(punishment.get('status', 'recorded')).title()}"
                    ),
                    "select_label": f"{action_label} · {rule_label}",
                    "select_value": f"punishment:{punishment['id']}",
                    "sort_key": str(punishment.get("created_at", "")),
                    "summary": (
                        f"`{punishment['id']}`\n"
                        f"{action_label} | {rule_label}\n"
                        f"{punishment.get('duration_text', 'Unknown')} | "
                        f"{str(punishment.get('status', 'recorded')).title()}\n"
                        f"{location}"
                    ),
                }
            )

        revoke_case_rows.sort(key=lambda item: str(item.get("sort_key", "")), reverse=True)

        return {
            "added_ticket_count": added_ticket_count,
            "all_involved_ticket_ids": all_involved_ticket_ids,
            "block": block,
            "caller_staff_level": caller_staff_level,
            "hidden_punishment_count": hidden_punishment_count,
            "hidden_ticket_count": hidden_ticket_count,
            "matching_punishments": matching_punishments,
            "owner_ticket_count": owner_ticket_count,
            "participant_ticket_count": participant_ticket_count,
            "revoke_case_rows": revoke_case_rows,
            "ticket_map": ticket_map,
            "visible_punishment_rows": visible_punishment_rows,
            "visible_ticket_rows": visible_ticket_rows,
        }

    async def _revoke_ticket_from_user_history(
        self,
        ticket_id: str,
        target_user_id: int,
        actor: discord.Member,
    ) -> dict[str, Any]:
        def updater(current: dict[str, Any]) -> dict[str, Any]:
            revocations = list(current.get("history_revocations", []))
            already_present = False
            for item in revocations:
                value = item.get("user_id") if isinstance(item, dict) else item
                try:
                    if int(value) == target_user_id:
                        already_present = True
                        break
                except (TypeError, ValueError):
                    continue
            if not already_present:
                revocations.append(
                    {
                        "revoked_at": discord.utils.utcnow().isoformat(),
                        "revoked_by": actor.id,
                        "user_id": target_user_id,
                    }
                )
            current["history_revocations"] = revocations
            return current

        updated = await self.bot.store.update_ticket(ticket_id, updater)
        await self.bot.transcripts.add_system_event(
            updated,
            "history_revoked",
            f"{actor} revoked this case from user {target_user_id}'s visible history.",
            actor_id=actor.id,
            extra={"target_user_id": target_user_id},
        )
        await self.bot.ticket_service.send_ticket_log(
            title="Case History Revoked",
            description=(
                f"Ticket: `{updated['channel_name']}`\n"
                f"Ticket ID: `{ticket_id}`\n"
                f"Removed From User History: `{target_user_id}`\n"
                f"By: {actor.mention}"
            ),
            color=discord.Color.orange(),
        )
        return updated

    async def _revoke_punishment_from_user_history(
        self,
        punishment_id: str,
        target_user_id: int,
        actor: discord.Member,
    ) -> dict[str, Any]:
        def updater(current: dict[str, Any]) -> dict[str, Any]:
            revocations = list(current.get("history_revocations", []))
            already_present = False
            for item in revocations:
                value = item.get("user_id") if isinstance(item, dict) else item
                try:
                    if int(value) == target_user_id:
                        already_present = True
                        break
                except (TypeError, ValueError):
                    continue
            if not already_present:
                revocations.append(
                    {
                        "revoked_at": discord.utils.utcnow().isoformat(),
                        "revoked_by": actor.id,
                        "user_id": target_user_id,
                    }
                )
            current["history_revocations"] = revocations
            return current

        updated = await self.bot.store.update_punishment(punishment_id, updater)
        await self.bot.punishment_service.send_punishment_log(
            title="Punishment History Revoked",
            description=(
                f"User ID: `{updated.get('user_id', target_user_id)}`\n"
                f"Punishment ID: `{punishment_id}`\n"
                f"Action: {str(updated.get('action', 'punishment')).title()}\n"
                f"Rule: {updated.get('rule_label') or updated.get('rule_id') or 'Manual / Unknown'}\n"
                f"Removed From User History: `{target_user_id}`\n"
                f"By: {actor.mention}"
            ),
            color=discord.Color.orange(),
        )
        return updated

    async def _resolve_support_member(self, user_id: int) -> discord.Member | None:
        guild = self.bot.get_guild(self.bot.config.support_guild_id)
        if guild is None:
            return None
        member = guild.get_member(user_id)
        if member is not None:
            return member
        try:
            return await guild.fetch_member(user_id)
        except discord.DiscordException:
            return None

    def _coerce_proof_payload(self, attachment: discord.Attachment) -> dict[str, Any] | None:
        content_type = (attachment.content_type or "").lower()
        suffix = Path(attachment.filename).suffix.lower()
        if not content_type.startswith("video/") and suffix not in self.video_extensions:
            return None
        return {
            "content_type": attachment.content_type or "",
            "filename": attachment.filename,
            "id": attachment.id,
            "size": attachment.size,
            "url": attachment.url,
        }

    def _build_source_context(self, interaction: discord.Interaction) -> dict[str, Any]:
        channel_name = getattr(interaction.channel, "name", "unknown-channel")
        guild_name = interaction.guild.name if interaction.guild is not None else "Unknown Guild"
        return {
            "channel_id": interaction.channel_id,
            "channel_name": channel_name,
            "guild_id": interaction.guild_id,
            "guild_name": guild_name,
        }

    def _parse_rule_family_tier(self, rule_id: str | None) -> tuple[str, int] | None:
        if not rule_id:
            return None
        parts = str(rule_id).split(".")
        if len(parts) < 2 or not parts[-1].isdigit():
            return None
        return ".".join(parts[:-1]), int(parts[-1])

    async def _apply_rule_escalation(
        self,
        user_id: int,
        rule: dict[str, Any],
    ) -> tuple[dict[str, Any], str | None]:
        parsed = self._parse_rule_family_tier(str(rule.get("id") or ""))
        if parsed is None:
            return rule, None

        family_id, selected_tier = parsed
        punishments = await self.bot.store.list_punishments()
        prior_matches: list[dict[str, Any]] = []
        highest_prior_tier = 0
        for punishment in punishments:
            if int(punishment.get("user_id", 0)) != user_id:
                continue
            prior_parsed = self._parse_rule_family_tier(str(punishment.get("rule_id") or ""))
            if prior_parsed is None or prior_parsed[0] != family_id:
                continue
            prior_matches.append(punishment)
            highest_prior_tier = max(highest_prior_tier, prior_parsed[1])

        if highest_prior_tier < selected_tier:
            return rule, None

        all_rules = await self.bot.store.list_rules()
        family_rules: dict[int, dict[str, Any]] = {}
        for candidate in all_rules:
            candidate_parsed = self._parse_rule_family_tier(str(candidate.get("id") or ""))
            if candidate_parsed is None or candidate_parsed[0] != family_id:
                continue
            family_rules[candidate_parsed[1]] = candidate

        suggested_tier = highest_prior_tier + 1
        latest_match = max(prior_matches, key=lambda item: str(item.get("created_at", "")))
        if suggested_tier not in family_rules:
            note = (
                "Offense history detected for this rule family.\n"
                f"Matching records found: {len(prior_matches)}\n"
                f"Latest matching record: {latest_match.get('rule_label') or latest_match.get('rule_id')}\n"
                "There is no higher configured tier in the rulebook, so the selected rule was left as-is."
            )
            return rule, note

        suggested_rule = family_rules[suggested_tier]
        note = (
            "Offense history detected for this rule family.\n"
            f"Matching records found: {len(prior_matches)}\n"
            f"Latest matching record: {latest_match.get('rule_label') or latest_match.get('rule_id')}\n"
            f"The preview was bumped from tier {selected_tier} to tier {suggested_tier}. "
            "Use Edit if you need something different."
        )
        return suggested_rule, note

    def _format_lift_option_label(self, punishment: dict[str, Any]) -> str:
        action = str(punishment.get("action", "punishment")).title()
        rule_label = str(punishment.get("rule_label") or punishment.get("rule_id") or "Unknown Rule")
        return f"{action} | {rule_label}"

    def _format_lift_option_description(self, punishment: dict[str, Any]) -> str:
        created_at = str(punishment.get("created_at", ""))[:10] or "Unknown"
        duration = str(punishment.get("duration_text") or "Unknown")
        return f"{duration} • {created_at}"

    def build_unban_review_embed(
        self,
        user_id: int,
        reason: str,
        punishments: list[dict[str, Any]],
        selected_punishment: dict[str, Any] | None,
        *,
        result: dict[str, Any] | None = None,
    ) -> discord.Embed:
        embed = discord.Embed(
            title="Lift Active Punishment",
            description=(
                f"Select the active mute or ban you want to lift for `{user_id}`.\n"
                f"Reason: {reason}"
            ),
            color=discord.Color.orange() if result is None else discord.Color.green(),
        )
        if selected_punishment is not None:
            embed.add_field(
                name="Selected Record",
                value=(
                    f"Action: {str(selected_punishment.get('action', 'punishment')).title()}\n"
                    f"Rule: {selected_punishment.get('rule_label') or selected_punishment.get('rule_id') or 'Unknown Rule'}\n"
                    f"Duration: {selected_punishment.get('duration_text', 'Unknown')}\n"
                    f"Created: {selected_punishment.get('created_at', 'Unknown')}"
                )[:1024],
                inline=False,
            )

        if punishments:
            overview_lines = [
                self._format_lift_option_label(punishment)
                for punishment in punishments[:10]
            ]
            embed.add_field(
                name="Active Punishments",
                value="\n".join(f"- {line}" for line in overview_lines)[:1024],
                inline=False,
            )
        else:
            embed.add_field(name="Active Punishments", value="No active bans or mutes were found.", inline=False)

        if result is not None:
            embed.add_field(
                name="Lift Result",
                value=(
                    f"Action Lifted: {str(result.get('action', 'punishment')).title()}\n"
                    f"Success: {result.get('success_count', 0)}/{len(result.get('results', []))}\n"
                    f"DM Status: {result.get('dm_status', 'unknown')}\n"
                    f"Records Lifted: {len(result.get('lifted_ids', []))}"
                )[:1024],
                inline=False,
            )
            embed.set_footer(text="This punishment lift has already been applied.")

    def _trial_mod_handoff_section(
        self,
        ticket: dict[str, Any] | None,
        source_context: dict[str, Any],
    ) -> TicketSection:
        if ticket:
            try:
                section = TicketSection(ticket["section"])
            except (KeyError, ValueError):
                section = None
            if section in {TicketSection.PGT, TicketSection.UGT}:
                return section

        guild_id = int(source_context.get("guild_id") or 0)
        if guild_id == self.bot.config.pgt_guild_id:
            return TicketSection.PGT
        return TicketSection.UGT

    def _build_trial_mod_handoff_embed(
        self,
        actor: discord.Member,
        user_id: int,
        rule: dict[str, Any],
        extra_comments: str | None,
        proof: dict[str, Any],
        source_context: dict[str, Any],
    ) -> discord.Embed:
        embed = discord.Embed(
            title="Trial Mod Punishment Handoff",
            description="A Trial Mod tried to run a ban-level punishment. This now needs Mod+ review.",
            color=discord.Color.orange(),
        )
        embed.add_field(name="Requested By", value=f"{actor.mention} (`{actor.id}`)", inline=False)
        embed.add_field(name="Target User ID", value=f"`{user_id}`", inline=True)
        embed.add_field(
            name="Rule",
            value=str(rule.get("label") or rule.get("id") or "Unknown Rule")[:1024],
            inline=True,
        )
        embed.add_field(
            name="Action",
            value=str(rule.get("action", "ban")).title()[:1024],
            inline=True,
        )
        embed.add_field(
            name="Duration",
            value=str(rule.get("duration_text") or "Unknown")[:1024],
            inline=True,
        )
        embed.add_field(
            name="Source",
            value=(
                f"{source_context.get('guild_name', 'Unknown Guild')} / "
                f"#{source_context.get('channel_name', 'unknown-channel')}"
            )[:1024],
            inline=True,
        )
        embed.add_field(
            name="Proof",
            value=(
                f"{proof.get('filename', 'Unknown file')}\n"
                f"{proof.get('url', 'No URL recorded')}"
            )[:1024],
            inline=False,
        )
        if extra_comments and extra_comments.strip():
            embed.add_field(name="Extra Comments", value=extra_comments.strip()[:1024], inline=False)
        embed.timestamp = discord.utils.utcnow()
        return embed

    async def _open_trial_mod_punishment_handoff(
        self,
        interaction: discord.Interaction,
        ticket: dict[str, Any] | None,
        user_id: int,
        rule: dict[str, Any],
        extra_comments: str | None,
        proof_attachment: discord.Attachment,
        proof_payload: dict[str, Any],
        source_context: dict[str, Any],
    ) -> TicketCreateResult:
        support_actor = await self._resolve_support_member(interaction.user.id)
        if support_actor is None:
            return TicketCreateResult(
                ok=False,
                message="I could not find your support-server membership, so I could not open the handoff ticket.",
            )

        section = self._trial_mod_handoff_section(ticket, source_context)
        result = await self.bot.ticket_service.create_ticket(support_actor, section, created_by=support_actor)
        if not result.ok or not result.ticket or not result.channel:
            return result

        await result.channel.send(
            embed=self.bot.ticket_service.build_ticket_embed(result.ticket, support_actor),
            view=self.bot.close_view,
        )
        handoff_embed = self._build_trial_mod_handoff_embed(
            support_actor,
            user_id,
            rule,
            extra_comments,
            proof_payload,
            source_context,
        )
        proof_file: discord.File | None = None
        try:
            proof_file = await proof_attachment.to_file()
        except discord.DiscordException:
            proof_file = None
        if proof_file is not None:
            await result.channel.send(embed=handoff_embed, file=proof_file)
        else:
            await result.channel.send(embed=handoff_embed)

        await self.bot.transcripts.add_system_event(
            result.ticket,
            "trial_mod_punishment_handoff",
            (
                f"{support_actor} escalated a {str(rule.get('action', 'ban')).title()} punishment for "
                f"user {user_id} using rule {rule.get('label') or rule.get('id') or 'Unknown Rule'}."
            ),
            actor_id=support_actor.id,
            extra={
                "extra_comments": extra_comments or "",
                "proof_filename": proof_payload.get("filename", ""),
                "proof_url": proof_payload.get("url", ""),
                "rule_id": rule.get("id"),
                "rule_label": rule.get("label"),
                "source_context": source_context,
                "target_user_id": user_id,
            },
        )
        await self.bot.ticket_service.send_ticket_log(
            title="Trial Mod Punishment Handoff",
            description=(
                f"Ticket: <#{result.channel.id}>\n"
                f"Requested by: {support_actor.mention}\n"
                f"Target User ID: `{user_id}`\n"
                f"Rule: {rule.get('label') or rule.get('id') or 'Unknown Rule'}\n"
                f"Action: {str(rule.get('action', 'ban')).title()}\n"
                f"Source: {source_context.get('guild_name', 'Unknown Guild')} / "
                f"#{source_context.get('channel_name', 'unknown-channel')}"
            ),
            color=discord.Color.orange(),
        )
        await self.bot.store.record_staff_action(support_actor.id, result.ticket["ticket_id"])
        return result

    async def build_punishment_review_embed(
        self,
        ticket: dict[str, Any] | None,
        user_id: int,
        rule: dict[str, Any],
        extra_comments: str | None,
        proof: dict[str, Any],
        source_context: dict[str, Any],
        *,
        escalation_note: str | None = None,
        punishment: dict[str, Any] | None = None,
    ) -> discord.Embed:
        history = await self.bot.store.list_punishments()
        matching = [item for item in history if int(item.get("user_id", 0)) == user_id]
        matching.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
        counts = {"ban": 0, "mute": 0, "warn": 0}
        for item in matching:
            action = str(item.get("action", "")).lower()
            if action in counts:
                counts[action] += 1

        embed = discord.Embed(
            title="Punishment Review" if not escalation_note else "Punishment Review - Escalated",
            description=(
                f"Review this punishment before anything is applied.\n\n"
                f"Target User ID: `{user_id}`\n"
                + (
                    f"Ticket: `{ticket['channel_name']}`\n"
                    f"Transcript: {ticket.get('transcript_url', 'Unavailable')}"
                    if ticket
                    else (
                        f"Guild: `{source_context.get('guild_name', 'Unknown Guild')}`\n"
                        f"Channel: `#{source_context.get('channel_name', 'unknown-channel')}`"
                    )
                )
            ),
            color=discord.Color.orange() if punishment is None else discord.Color.red(),
        )
        embed.add_field(
            name="Pending Action",
            value=(
                f"Rule: {rule.get('label', rule.get('id', 'Unknown Rule'))}\n"
                f"Action: {str(rule.get('action', 'ban')).title()}\n"
                f"Reason: {rule.get('reason') or rule.get('label') or 'Rule punishment'}\n"
                f"Duration: {rule.get('duration_text') or 'Custom'}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Proof Video",
            value=(
                f"{proof.get('filename', 'Unknown file')}\n"
                f"{proof.get('url', 'No URL recorded')}"
            )[:1024],
            inline=False,
        )
        embed.add_field(
            name="Past History",
            value=(
                f"Bans: {counts['ban']}\n"
                f"Mutes: {counts['mute']}\n"
                f"Warns: {counts['warn']}\n"
                f"Total Records: {len(matching)}"
            ),
            inline=False,
        )
        if extra_comments and extra_comments.strip():
            embed.add_field(name="Extra Comments", value=extra_comments.strip()[:1024], inline=False)
        if escalation_note:
            embed.add_field(name="Escalation Check", value=escalation_note[:1024], inline=False)
            embed.color = discord.Color.gold() if punishment is None else discord.Color.red()

        if matching:
            recent_lines: list[str] = []
            for item in matching[:5]:
                created_at = str(item.get("created_at", ""))[:10] or "Unknown"
                action = str(item.get("action", "punishment")).title()
                duration = str(item.get("duration_text", "Unknown"))
                status = str(item.get("status", "recorded")).title()
                reason = str(item.get("reason") or item.get("rule_label") or "No reason").strip()
                recent_lines.append(
                    f"{created_at} | {action} | {duration} | {status}\n{reason[:85]}"
                )
            embed.add_field(name="Recent History", value="\n\n".join(recent_lines)[:1024], inline=False)
        else:
            embed.add_field(name="Recent History", value="No prior punishments were found.", inline=False)

        if punishment is not None:
            result_lines = [
                f"Recorded Action: {punishment['action'].title()}",
                f"Duration: {punishment['duration_text']}",
                f"DM Status: {punishment.get('dm_status', 'unknown')}",
            ]
            if punishment["action"] == "ban":
                result_lines.append(
                    f"Ban success: {punishment.get('ban_success_count', 0)}/"
                    f"{len(punishment.get('ban_results', []))}"
                )
            elif punishment["action"] == "mute":
                result_lines.append(
                    f"Mute success: {punishment.get('mute_success_count', 0)}/"
                    f"{max(len(punishment.get('mute_results', [])), 1)}"
                )
            embed.add_field(name="Applied Result", value="\n".join(result_lines), inline=False)
            embed.set_footer(text="This punishment has already been applied.")

        return embed

    @app_commands.command(name="moveticket", description="Move the current ticket to another section.")
    @app_commands.guild_only()
    @app_commands.describe(section="Where the current ticket should be moved")
    @app_commands.choices(
        section=[
            app_commands.Choice(name="UGT", value=TicketSection.UGT.value),
            app_commands.Choice(name="PGT", value=TicketSection.PGT.value),
            app_commands.Choice(name="Management", value=TicketSection.MANAGEMENT.value),
            app_commands.Choice(name="Appeal", value=TicketSection.APPEAL.value),
        ]
    )
    async def moveticket(self, interaction: discord.Interaction, section: str) -> None:
        if not await self._require_level(interaction, StaffLevel.MOD):
            return
        ticket, channel = await self._require_ticket(interaction)
        if not ticket or not channel:
            return
        await interaction.response.defer(ephemeral=True)
        updated = await self.bot.ticket_service.move_ticket(
            ticket,
            channel,
            TicketSection(section),
            interaction.user,
        )
        await interaction.followup.send(
            f"Moved this ticket to {TicketSection(updated['section']).label}.",
            ephemeral=True,
        )

    @app_commands.command(name="punish", description="Punish a user with a rule and required video proof.")
    @app_commands.guild_only()
    @app_commands.describe(
        user_id="Discord user ID",
        rule="Rule from the rulebook",
        extra_comments="Optional extra notes to include",
        proof_video="Required video proof attachment",
    )
    async def punish(
        self,
        interaction: discord.Interaction,
        user_id: str,
        rule: str,
        proof_video: discord.Attachment,
        extra_comments: str | None = None,
    ) -> None:
        if not await self._require_punish_level(interaction, StaffLevel.TRIAL_MOD):
            return
        ticket = await self._optional_ticket(interaction)
        parsed_user_id = sanitize_user_id(user_id)
        selected_rule = await self.bot.store.get_rule(rule)
        if not selected_rule:
            await interaction.response.send_message("That rule was not found in `data/rules.json`.", ephemeral=True)
            return
        proof_payload = self._coerce_proof_payload(proof_video)
        if proof_payload is None:
            await interaction.response.send_message(
                "You must attach a video file as proof when using `/punish`.",
                ephemeral=True,
            )
            return
        source_context = self._build_source_context(interaction)
        selected_rule, escalation_note = await self._apply_rule_escalation(parsed_user_id, selected_rule)
        selected_action = str(selected_rule.get("action", "ban")).lower()
        staff_level = await self._support_staff_level(interaction.user.id)
        if staff_level == StaffLevel.TRIAL_MOD and selected_action not in {"mute", "warn"}:
            await interaction.response.defer(ephemeral=True)
            handoff = await self._open_trial_mod_punishment_handoff(
                interaction,
                ticket,
                parsed_user_id,
                selected_rule,
                extra_comments,
                proof_video,
                proof_payload,
                source_context,
            )
            if not handoff.ok or not handoff.channel:
                await interaction.followup.send(
                    handoff.message or "I could not open the handoff ticket.",
                    ephemeral=True,
                )
                return
            await interaction.followup.send(
                (
                    "Trial Mods can only apply mute and warn punishments directly.\n"
                    f"I opened a handoff ticket for Mod+ review: {handoff.channel.mention}"
                ),
                ephemeral=True,
            )
            return
        if selected_action == "manual":
            notes = str(selected_rule.get("notes", "")).strip()
            message = (
                "That rule is marked for manual staff handling, so `/punish` will not run it automatically.\n"
                "Use `/manual-ban` in this ticket instead if this should be a ban.\n"
                f"Reason: {selected_rule.get('reason') or selected_rule.get('label') or 'Manual staff action'}\n"
                f"Suggested duration: {selected_rule.get('duration_text') or 'Set the duration manually.'}"
            )
            if notes:
                message = f"{message}\nNotes: {notes}"
            await interaction.response.send_message(message, ephemeral=True)
            return
        embed = await self.build_punishment_review_embed(
            ticket,
            parsed_user_id,
            selected_rule,
            extra_comments,
            proof_payload,
            source_context,
            escalation_note=escalation_note,
        )
        view = PunishmentReviewView(
            self,
            interaction,
            ticket,
            interaction.user,
            parsed_user_id,
            selected_rule,
            extra_comments,
            proof_payload,
            source_context,
            escalation_note,
        )
        await interaction.response.send_message(
            embed=embed,
            view=view,
            ephemeral=True,
        )

    @punish.autocomplete("rule")
    async def punish_rule_autocomplete(
        self,
        _: discord.Interaction,
        current: str,
    ) -> list[app_commands.Choice[str]]:
        rules = await self.bot.store.list_rules()
        lowered = current.lower()
        choices: list[app_commands.Choice[str]] = []
        for rule in rules:
            text = " ".join(
                [
                    str(rule.get("id", "")),
                    str(rule.get("label", "")),
                    str(rule.get("reason", "")),
                ]
            ).lower()
            if lowered and lowered not in text:
                continue
            label = f"{rule.get('label', rule.get('id', 'Rule'))} ({rule.get('duration_text', 'custom')})"
            if rule.get("action"):
                action_label = {
                    "ban": "BAN",
                    "manual": "STAFF ACTION",
                    "mute": "MUTE",
                    "warn": "WARN",
                }.get(str(rule.get("action")).lower(), str(rule.get("action")).upper())
                label = f"{label} [{action_label}]"
            choices.append(app_commands.Choice(name=label[:100], value=str(rule.get("id"))))
            if len(choices) >= 25:
                break
        return choices

    @app_commands.command(name="unban", description="Unban a user from the configured servers.")
    @app_commands.guild_only()
    @app_commands.describe(user_id="Discord user ID", reason="Why they are being unbanned")
    async def unban(self, interaction: discord.Interaction, user_id: str, reason: str) -> None:
        if not await self._require_level(interaction, StaffLevel.LEAGUE_MANAGER):
            return
        parsed_user_id = sanitize_user_id(user_id)
        punishments = await self.bot.punishment_service.list_active_liftable_punishments(parsed_user_id)
        if not punishments:
            await interaction.response.send_message(
                "That user does not have any active ban or mute punishments to lift.",
                ephemeral=True,
            )
            return
        embed = self.build_unban_review_embed(
            parsed_user_id,
            reason,
            punishments[:25],
            punishments[0],
        )
        view = UnbanReviewView(
            self,
            interaction,
            interaction.user,
            parsed_user_id,
            reason,
            punishments,
        )
        await interaction.response.send_message(
            embed=embed,
            view=view,
            ephemeral=True,
        )

    @app_commands.command(name="hacked", description="Kick a user from the configured league servers because their account appears hacked.")
    @app_commands.guild_only()
    @app_commands.describe(user_id="Discord user ID")
    async def hacked(self, interaction: discord.Interaction, user_id: str) -> None:
        if not await self._require_punish_level(interaction, StaffLevel.TRIAL_MOD):
            return

        parsed_user_id = sanitize_user_id(user_id)
        ticket = await self._optional_ticket(interaction)
        await interaction.response.defer(ephemeral=True)
        await interaction.edit_original_response(
            content=(
                f"Running hacked-account cleanup for `{parsed_user_id}`.\n"
                "This can take a bit while I remove recent messages from PGT and UGT."
            ),
            embed=None,
            view=None,
        )
        punishment = await self.bot.punishment_service.hacked_kick(
            ticket,
            interaction.user,
            parsed_user_id,
            self._build_source_context(interaction),
        )
        kick_results = punishment.get("kick_results", [])
        await interaction.edit_original_response(
            content=(
                f"Hacked-account kick recorded for `{parsed_user_id}`.\n"
                f"Kick success: {punishment.get('kick_success_count', 0)}/{len(kick_results)}\n"
                f"Messages deleted: {punishment.get('purged_message_count', 0)}"
            ),
        )

    @app_commands.command(
        name="contact",
        description="DM a user to join, or open a management ticket if they are already here.",
    )
    @app_commands.guild_only()
    @app_commands.describe(user_id="Discord user ID")
    async def contact(self, interaction: discord.Interaction, user_id: str) -> None:
        if not await self._require_level(interaction, StaffLevel.SUPERVISOR):
            return
        parsed_user_id = sanitize_user_id(user_id)
        target_member = await self._resolve_support_member(parsed_user_id)
        if target_member is None:
            await interaction.response.defer(ephemeral=True)
            ok, message = await self.bot.punishment_service.contact_user(
                interaction.user,
                parsed_user_id,
            )
            await interaction.followup.send(message if ok else f"Contact failed: {message}", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        result = await self.bot.ticket_service.ensure_management_contact_ticket(interaction.user, target_member)
        if not result.ok or not result.ticket or not result.channel:
            await interaction.followup.send(result.message, ephemeral=True)
            return
        if result.message == "Management contact ticket opened.":
            embed = self.bot.ticket_service.build_ticket_embed(result.ticket, target_member)
            await result.channel.send(embed=embed, view=self.bot.close_view)
        await interaction.followup.send(
            f"Management contact ticket ready: {result.channel.mention}",
            ephemeral=True,
        )

    @app_commands.command(name="add-user", description="Add another user to the current ticket.")
    @app_commands.guild_only()
    @app_commands.describe(user_id="Discord user ID to add")
    async def add_user(self, interaction: discord.Interaction, user_id: str) -> None:
        if not await self._require_level(interaction, StaffLevel.TRIAL_MOD):
            return
        ticket, channel = await self._require_ticket(interaction)
        if not ticket or not channel:
            return
        if ticket["state"] != TicketState.OPEN.value:
            await interaction.response.send_message(
                "Only open tickets can have users added to them.",
                ephemeral=True,
            )
            return
        parsed_user_id = sanitize_user_id(user_id)
        target_member = await self._resolve_support_member(parsed_user_id)
        if target_member is None:
            await interaction.response.send_message(
                "That user is not in the support server right now. Have them join first, or use `/contact`.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        updated, added = await self.bot.ticket_service.add_user_to_ticket(
            ticket,
            channel,
            target_member,
            interaction.user,
        )
        await interaction.followup.send(
            (
                f"Added {target_member.mention} to {channel.mention}."
                if added
                else f"{target_member.mention} already had ticket access."
            ),
            ephemeral=True,
        )

    @app_commands.command(name="rename", description="Rename the current ticket.")
    @app_commands.guild_only()
    @app_commands.describe(new_name="Short ticket name")
    async def rename(self, interaction: discord.Interaction, new_name: str) -> None:
        if not await self._require_level(interaction, StaffLevel.TRIAL_MOD):
            return
        ticket, channel = await self._require_ticket(interaction)
        if not ticket or not channel:
            return
        await interaction.response.defer(ephemeral=True)
        updated = await self.bot.ticket_service.rename_ticket(ticket, channel, new_name, interaction.user)
        await interaction.followup.send(f"Ticket renamed to `{updated['channel_name']}`.", ephemeral=True)

    @app_commands.command(name="close-request", description="Ask the ticket owner if the ticket can be closed.")
    @app_commands.guild_only()
    async def close_request(self, interaction: discord.Interaction) -> None:
        if not await self._require_level(interaction, StaffLevel.MOD):
            return
        ticket, channel = await self._require_ticket(interaction)
        if not ticket or not channel:
            return
        message = self.bot.ticket_service.build_close_request_message(ticket["owner_id"])
        await channel.send(message, view=self.bot.close_view)
        await interaction.response.send_message("Close request posted.", ephemeral=True)

    @app_commands.command(name="about", description="Show the available moderation commands.")
    @app_commands.guild_only()
    async def about(self, interaction: discord.Interaction) -> None:
        if not await self._require_level(interaction, StaffLevel.TRIAL_MOD):
            return
        embed = discord.Embed(title="UGT & PGT Manager Commands", color=discord.Color.blurple())
        embed.description = (
            "`/moveticket` Move the current ticket to another section.\n"
            "`/punish` Rule-based punishment with required video proof. Trial Mods can apply mutes/warns and auto-handoff ban requests.\n"
            "`/hacked` Kick a hacked account from the configured league servers, delete their recent messages, and log it.\n"
            "`/manual-ban` Manual ban linked to the current ticket.\n"
            "`/unban` Remove a ban from the configured servers.\n"
            "`/contact` DM a user to join, or open a management contact ticket for them.\n"
            "`/user-info` Show logged tickets, punishments, proof links, and block status for a user.\n"
            "`/add-user` Add another user to the current ticket.\n"
            "`/rename` Rename the current ticket.\n"
            "`/close-request` Ask if the ticket can be closed.\n"
            "`/block` Block a user from support and management tickets.\n"
            "`/unblock` Remove a support and management block.\n"
            "`/stats` Show ticket activity leaderboards.\n"
            "`/refreshpanel` Re-post or update the ticket panel."
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="user-info", description="Show logged tickets, punishments, and blocks for a user.")
    @app_commands.guild_only()
    @app_commands.describe(user_id="Discord user ID")
    async def user_info(self, interaction: discord.Interaction, user_id: str) -> None:
        if not await self._require_punish_level(interaction, StaffLevel.TRIAL_MOD):
            return

        target_user_id = sanitize_user_id(user_id)
        await interaction.response.defer(ephemeral=True)

        payload = await self._build_user_history_payload(target_user_id, interaction.user.id)
        block = payload["block"]
        visible_ticket_rows = payload["visible_ticket_rows"]
        hidden_ticket_count = payload["hidden_ticket_count"]
        owner_ticket_count = payload["owner_ticket_count"]
        participant_ticket_count = payload["participant_ticket_count"]
        added_ticket_count = payload["added_ticket_count"]
        all_involved_ticket_ids = payload["all_involved_ticket_ids"]
        matching_punishments = payload["matching_punishments"]
        visible_punishment_rows = payload["visible_punishment_rows"]
        hidden_punishment_count = payload["hidden_punishment_count"]

        block_status = "Blocked" if block else "Not blocked"
        block_reason = block["reason"] if block else "None"
        staff_stats = await self.bot.store.get_staff_stats()
        staff_entry = staff_stats.get(str(target_user_id))

        embed = discord.Embed(
            title="User Info",
            description=(
                f"User ID: `{target_user_id}`\n"
                f"Support Block: {block_status}\n"
                f"Block Reason: {block_reason}"
            ),
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Tickets",
            value=(
                f"Total involved: {len(all_involved_ticket_ids)}\n"
                f"Owner tickets: {owner_ticket_count}\n"
                f"Participant tickets: {participant_ticket_count}\n"
                f"Added-user tickets: {added_ticket_count}\n"
                f"Hidden by access: {hidden_ticket_count}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Punishments",
            value=(
                f"Total records: {len(matching_punishments)}\n"
                f"Visible records: {len(visible_punishment_rows)}\n"
                f"Hidden by access: {hidden_punishment_count}"
            ),
            inline=False,
        )
        if staff_entry:
            embed.add_field(
                name="Staff Activity",
                value=(
                    f"Handled tickets: {len(staff_entry.get('handled_tickets', []))}\n"
                    f"Spoken tickets: {len(staff_entry.get('spoken_tickets', []))}\n"
                    f"Closed tickets: {len(staff_entry.get('closed_tickets', []))}"
                ),
                inline=False,
            )
        if visible_punishment_rows:
            embed.add_field(
                name="Recent Punishments",
                value="\n\n".join(visible_punishment_rows[:3])[:1024],
                inline=False,
            )
        elif matching_punishments:
            embed.add_field(name="Recent Punishments", value="All punishment details are hidden by access rules.", inline=False)
        else:
            embed.add_field(name="Recent Punishments", value="No punishment records found.", inline=False)

        report_lines = [
            f"User Info Report for {target_user_id}",
            "",
            f"Support Block: {block_status}",
            f"Block Reason: {block_reason}",
            "",
            "Staff Activity:",
        ]
        if staff_entry:
            report_lines.extend(
                [
                    f"- Handled tickets: {len(staff_entry.get('handled_tickets', []))}",
                    f"- Spoken tickets: {len(staff_entry.get('spoken_tickets', []))}",
                    f"- Closed tickets: {len(staff_entry.get('closed_tickets', []))}",
                ]
            )
        else:
            report_lines.append("- No staff activity logged.")

        report_lines.extend([
            "",
            "Tickets:",
        ])
        if visible_ticket_rows:
            report_lines.extend(visible_ticket_rows)
        else:
            report_lines.append("- No visible tickets found.")
        if hidden_ticket_count:
            report_lines.append(f"- Hidden tickets due to transcript access: {hidden_ticket_count}")

        report_lines.extend(["", "Punishments:"])
        if visible_punishment_rows:
            report_lines.extend(visible_punishment_rows)
        else:
            report_lines.append("- No visible punishment records found.")
        if hidden_punishment_count:
            report_lines.append(f"- Hidden punishments due to transcript access: {hidden_punishment_count}")

        report = io.BytesIO("\n".join(report_lines).encode("utf-8"))
        file = discord.File(report, filename=f"user-info-{target_user_id}.txt")
        view: UserInfoActionsView | None = None
        if (
            isinstance(interaction.user, discord.Member)
            and payload["caller_staff_level"] >= StaffLevel.LEAGUE_MANAGER
        ):
            view = UserInfoActionsView(
                self,
                interaction.user,
                target_user_id,
                payload["revoke_case_rows"],
                allow_revoke=True,
            )
        await interaction.followup.send(embed=embed, file=file, view=view, ephemeral=True)

    @app_commands.command(name="block", description="Block a user from support and management tickets.")
    @app_commands.guild_only()
    @app_commands.describe(user_id="Discord user ID", reason="Why they are blocked")
    async def block(self, interaction: discord.Interaction, user_id: str, reason: str) -> None:
        if not await self._require_level(interaction, StaffLevel.SUPERVISOR):
            return
        await interaction.response.defer(ephemeral=True)
        await self.bot.punishment_service.block_user(interaction.user, sanitize_user_id(user_id), reason)
        await interaction.followup.send("User blocked from support and management tickets.", ephemeral=True)

    @app_commands.command(name="unblock", description="Unblock a user from support and management tickets.")
    @app_commands.guild_only()
    @app_commands.describe(user_id="Discord user ID")
    async def unblock(self, interaction: discord.Interaction, user_id: str) -> None:
        if not await self._require_level(interaction, StaffLevel.SUPERVISOR):
            return
        await interaction.response.defer(ephemeral=True)
        removed = await self.bot.punishment_service.unblock_user(
            interaction.user,
            sanitize_user_id(user_id),
        )
        if removed:
            await interaction.followup.send("User unblocked from support and management tickets.", ephemeral=True)
        else:
            await interaction.followup.send("That user is not currently blocked.", ephemeral=True)

    @app_commands.command(name="stats", description="Show ticket activity stats for staff.")
    @app_commands.guild_only()
    async def stats(self, interaction: discord.Interaction) -> None:
        if not await self._require_level(interaction, StaffLevel.TRIAL_MOD):
            return
        staff_stats = await self.bot.store.get_staff_stats()
        if not staff_stats:
            await interaction.response.send_message("No staff stats have been recorded yet.", ephemeral=True)
            return

        def top_line(bucket: str) -> str:
            ranked = sorted(
                staff_stats.items(),
                key=lambda item: len(item[1].get(bucket, [])),
                reverse=True,
            )
            lines = []
            for staff_id, values in ranked[:5]:
                lines.append(f"<@{staff_id}> - {len(values.get(bucket, []))}")
            return "\n".join(lines) or "No data yet."

        embed = discord.Embed(title="Ticket Stats", color=discord.Color.green())
        embed.add_field(name="Most Tickets Spoken In", value=top_line("spoken_tickets"), inline=False)
        embed.add_field(name="Most Tickets Closed", value=top_line("closed_tickets"), inline=False)
        embed.add_field(name="Most Tickets Handled Overall", value=top_line("handled_tickets"), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="manual-ban", description="Manually ban a user from the current ticket.")
    @app_commands.guild_only()
    @app_commands.describe(
        user_id="Discord user ID",
        duration="Example: 1m, 1h, 1d, 1w, 1mo, 1y, permanent",
        reason="Why they are being manually banned",
    )
    async def manual_ban(
        self,
        interaction: discord.Interaction,
        user_id: str,
        duration: str,
        reason: str,
    ) -> None:
        if not await self._require_level(interaction, StaffLevel.LEAGUE_MANAGER):
            return
        ticket, _ = await self._require_ticket(interaction)
        if not ticket:
            return
        try:
            parse_duration(duration)
        except ValueError:
            await interaction.response.send_message(
                (
                    "That duration was not valid.\n"
                    "Use short units like:\n"
                    "`1m` = 1 minute\n"
                    "`1h` = 1 hour\n"
                    "`1d` = 1 day\n"
                    "`1w` = 1 week\n"
                    "`1mo` = 1 month\n"
                    "`1y` = 1 year\n"
                    "You can also use `permanent`."
                ),
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        punishment = await self.bot.punishment_service.manual_ban(
            ticket,
            interaction.user,
            sanitize_user_id(user_id),
            duration,
            reason,
        )
        await interaction.followup.send(
            (
                f"Manual ban logged for `{punishment['user_id']}`. Duration: {punishment['duration_text']}.\n"
                f"Ban success: {punishment.get('ban_success_count', 0)}/"
                f"{len(punishment.get('ban_results', []))}"
            ),
            ephemeral=True,
        )

    @app_commands.command(name="refreshpanel", description="Refresh the ticket panel message.")
    @app_commands.guild_only()
    async def refreshpanel(self, interaction: discord.Interaction) -> None:
        if not await self._require_level(interaction, StaffLevel.LEAGUE_MANAGER):
            return
        await interaction.response.defer(ephemeral=True)
        message = await self.bot.ticket_service.ensure_panel(self.bot.panel_view)
        await interaction.followup.send(f"Panel refreshed in <#{message.channel.id}>.", ephemeral=True)


class ManagerBot(commands.Bot):
    def __init__(self, config: Config) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = config.enable_members_intent
        intents.messages = True
        intents.message_content = config.enable_message_content_intent

        super().__init__(command_prefix="!", intents=intents)
        self.config = config
        self.store = JsonStore(config)
        self.transcripts = TranscriptStore(config)
        self.ticket_service = TicketService(self, config, self.store, self.transcripts)
        self.punishment_service = PunishmentService(self, config, self.store)
        self.panel_view = TicketPanelView(self)
        self.close_view = CloseTicketView(self)
        self.delete_view = DeleteTicketView(self)
        self.managed_guild_ids = {
            guild_id
            for guild_id in {config.support_guild_id, config.pgt_guild_id, config.ugt_guild_id}
            if guild_id
        }
        self.command_guild_objects = [discord.Object(id=guild_id) for guild_id in sorted(self.managed_guild_ids)]
        self.support_guild_object = discord.Object(id=config.support_guild_id)
        self.panel_ready = False
        self.oauth_sessions: dict[str, dict[str, Any]] = {}

    async def setup_hook(self) -> None:
        await self.store.initialize()
        await self.add_cog(ManagerCog(self))
        self.add_view(self.panel_view)
        self.add_view(self.close_view)
        self.add_view(self.delete_view)
        for guild_object in self.command_guild_objects:
            self.tree.copy_global_to(guild=guild_object)
            await self.tree.sync(guild=guild_object)
        self.auto_unban_loop.start()

    async def on_ready(self) -> None:
        if not self.panel_ready:
            await self.ticket_service.ensure_panel(self.panel_view)
            self.panel_ready = True
        print(f"Logged in as {self.user} ({self.user.id})")

    async def create_ticket_from_button(
        self,
        interaction: discord.Interaction,
        section: TicketSection,
    ) -> None:
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This button only works in the support server.", ephemeral=True)
            return
        if interaction.guild_id != self.config.support_guild_id:
            await interaction.response.send_message("This panel only works in the support server.", ephemeral=True)
            return
        result = await self.ticket_service.create_ticket(interaction.user, section)
        if not result.ok or not result.ticket or not result.channel:
            if interaction.response.is_done():
                await interaction.followup.send(result.message, ephemeral=True)
            else:
                await interaction.response.send_message(result.message, ephemeral=True)
            return

        embed = self.ticket_service.build_ticket_embed(result.ticket, interaction.user)
        await result.channel.send(embed=embed, view=self.close_view)
        await self.ticket_service.send_ticket_log(
            title="Ticket Opened",
            description=(
                f"Ticket: <#{result.channel.id}>\n"
                f"Owner: {interaction.user.mention}\n"
                f"Type: {section.label}"
            ),
            color=discord.Color.green(),
        )
        if interaction.response.is_done():
            await interaction.followup.send(f"Your ticket has been created: {result.channel.mention}", ephemeral=True)
        else:
            await interaction.response.send_message(
                f"Your ticket has been created: {result.channel.mention}",
                ephemeral=True,
            )

    async def handle_close_button(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("That button only works inside a ticket.", ephemeral=True)
            return
        ticket = await self.store.get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message("That channel is not a tracked ticket.", ephemeral=True)
            return
        if ticket["state"] != TicketState.OPEN.value:
            await interaction.response.send_message("This ticket is already closed.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        updated = await self.ticket_service.close_ticket(ticket, interaction.channel, interaction.user)
        close_notice = await interaction.channel.send(
            self.ticket_service.build_delete_prompt(updated),
            view=self.delete_view,
        )
        await self.store.update_ticket(
            updated["ticket_id"],
            lambda current: {
                **current,
                "close_notice_message_id": close_notice.id,
                "owner_transcript_cutoff_at": close_notice.created_at.isoformat(),
            },
        )
        await interaction.followup.send("Ticket closed.", ephemeral=True)

    async def handle_delete_button(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Only staff can delete tickets.", ephemeral=True)
            return
        if get_staff_level(interaction.user, self.config) < StaffLevel.TRIAL_MOD:
            await interaction.response.send_message("Only Trial Mods or higher can delete tickets.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("That button only works inside a ticket.", ephemeral=True)
            return
        ticket = await self.store.get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message("That channel is not a tracked ticket.", ephemeral=True)
            return
        await interaction.response.send_message("Deleting ticket...", ephemeral=True)
        await self.ticket_service.delete_ticket(ticket, interaction.channel, interaction.user)

    async def on_message(self, message: discord.Message) -> None:
        if message.guild is None or message.guild.id != self.config.support_guild_id:
            return
        if not isinstance(message.channel, discord.TextChannel):
            return
        if message.author.bot:
            return
        ticket = await self.store.get_ticket_by_channel(message.channel.id)
        if not ticket:
            return
        await self.transcripts.record_message(ticket, message)
        if isinstance(message.author, discord.Member):
            if get_staff_level(message.author, self.config) >= StaffLevel.TRIAL_MOD:
                await self.store.record_staff_message(message.author.id, ticket["ticket_id"])
                await self.store.record_staff_action(message.author.id, ticket["ticket_id"])
        if message.content.strip().lower() == "!evidence":
            await message.channel.send(
                self.ticket_service.build_evidence_message(ticket["owner_id"]),
            )
            try:
                await message.delete()
            except discord.DiscordException:
                pass

    async def on_message_edit(self, before: discord.Message, after: discord.Message) -> None:
        if after.guild is None or after.guild.id != self.config.support_guild_id:
            return
        if not isinstance(after.channel, discord.TextChannel):
            return
        ticket = await self.store.get_ticket_by_channel(after.channel.id)
        if not ticket:
            return
        await self.transcripts.record_edit(ticket, before, after)

    async def on_message_delete(self, message: discord.Message) -> None:
        if message.guild is None or message.guild.id != self.config.support_guild_id:
            return
        if not isinstance(message.channel, discord.TextChannel):
            return
        ticket = await self.store.get_ticket_by_channel(message.channel.id)
        if not ticket:
            return
        await self.transcripts.record_delete(ticket["ticket_id"], message.id)

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent) -> None:
        if payload.guild_id != self.config.support_guild_id or payload.cached_message is not None:
            return
        ticket = await self.store.get_ticket_by_channel(payload.channel_id)
        if not ticket:
            return
        await self.transcripts.record_delete(ticket["ticket_id"], payload.message_id)

    @tasks.loop(minutes=1)
    async def auto_unban_loop(self) -> None:
        if not self.is_ready():
            return
        await self.punishment_service.auto_unban_due_users()
        await self.punishment_service.auto_update_mutes()

    @auto_unban_loop.before_loop
    async def before_auto_unban_loop(self) -> None:
        await self.wait_until_ready()
