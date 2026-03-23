from __future__ import annotations

from typing import Iterable

import discord

from .config import Config
from .constants import StaffLevel, TicketSection, TicketState


def get_staff_level(member: discord.Member | None, config: Config) -> StaffLevel:
    if member is None:
        return StaffLevel.NONE
    role_ids = {role.id for role in member.roles}
    if config.league_manager_role_id in role_ids:
        return StaffLevel.LEAGUE_MANAGER
    if config.supervisor_role_id in role_ids:
        return StaffLevel.SUPERVISOR
    if config.mod_role_id in role_ids:
        return StaffLevel.MOD
    if config.trial_mod_role_id in role_ids:
        return StaffLevel.TRIAL_MOD
    return StaffLevel.NONE


def is_staff_level(level: StaffLevel) -> bool:
    return level >= StaffLevel.TRIAL_MOD


def get_required_level_for_section(section: TicketSection) -> StaffLevel:
    if section in {TicketSection.PGT, TicketSection.UGT}:
        return StaffLevel.MOD
    if section == TicketSection.APPEAL:
        return StaffLevel.SUPERVISOR
    return StaffLevel.LEAGUE_MANAGER


def get_section_category_id(section: TicketSection, config: Config) -> int:
    return {
        TicketSection.APPEAL: config.appeal_category_id,
        TicketSection.MANAGEMENT: config.management_category_id,
        TicketSection.PGT: config.pgt_category_id,
        TicketSection.UGT: config.ugt_category_id,
    }[section]


def build_channel_overwrites(
    guild: discord.Guild,
    config: Config,
    section: TicketSection,
    state: TicketState,
    owner: discord.Member | None,
    owner_is_staff: bool,
    extra_members: Iterable[discord.Member] = (),
) -> dict[discord.abc.Snowflake, discord.PermissionOverwrite]:
    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
    }
    if guild.me is not None:
        overwrites[guild.me] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            manage_channels=True,
            manage_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        )

    def set_role(role_id: int, *, view: bool, send: bool) -> None:
        role = guild.get_role(role_id)
        if role is None:
            return
        overwrites[role] = discord.PermissionOverwrite(
            view_channel=view,
            send_messages=send,
            read_message_history=view,
            attach_files=send,
            embed_links=send,
        )

    if state == TicketState.OPEN:
        if section in {TicketSection.PGT, TicketSection.UGT}:
            set_role(config.trial_mod_role_id, view=True, send=False)
            set_role(config.mod_role_id, view=True, send=True)
            set_role(config.supervisor_role_id, view=True, send=True)
            set_role(config.league_manager_role_id, view=True, send=True)
        elif section == TicketSection.APPEAL:
            set_role(config.trial_mod_role_id, view=False, send=False)
            set_role(config.mod_role_id, view=False, send=False)
            set_role(config.supervisor_role_id, view=True, send=True)
            set_role(config.league_manager_role_id, view=True, send=True)
        else:
            set_role(config.trial_mod_role_id, view=False, send=False)
            set_role(config.mod_role_id, view=False, send=False)
            set_role(config.supervisor_role_id, view=False, send=False)
            set_role(config.league_manager_role_id, view=True, send=True)
    elif state == TicketState.CLOSED:
        if section in {TicketSection.PGT, TicketSection.UGT}:
            set_role(config.trial_mod_role_id, view=True, send=True)
            set_role(config.mod_role_id, view=True, send=True)
            set_role(config.supervisor_role_id, view=True, send=True)
            set_role(config.league_manager_role_id, view=True, send=True)
        elif section == TicketSection.APPEAL:
            set_role(config.trial_mod_role_id, view=False, send=False)
            set_role(config.mod_role_id, view=False, send=False)
            set_role(config.supervisor_role_id, view=True, send=True)
            set_role(config.league_manager_role_id, view=True, send=True)
        else:
            set_role(config.trial_mod_role_id, view=False, send=False)
            set_role(config.mod_role_id, view=False, send=False)
            set_role(config.supervisor_role_id, view=False, send=False)
            set_role(config.league_manager_role_id, view=True, send=True)

    def set_member(member: discord.Member, *, is_staff_member: bool) -> None:
        member_send = state == TicketState.OPEN or is_staff_member
        if state == TicketState.CLOSED and not is_staff_member:
            member_send = False
        member_view = is_staff_member or state != TicketState.DELETED
        if state == TicketState.CLOSED and not is_staff_member:
            member_view = False
        overwrites[member] = discord.PermissionOverwrite(
            view_channel=member_view,
            send_messages=member_send,
            read_message_history=member_view,
            attach_files=member_send,
            embed_links=member_send,
        )

    if owner is not None:
        owner_send = state == TicketState.OPEN or owner_is_staff
        if state == TicketState.CLOSED and not owner_is_staff:
            owner_send = False
        owner_view = owner_is_staff or state != TicketState.DELETED
        if state == TicketState.CLOSED and not owner_is_staff:
            owner_view = False
        overwrites[owner] = discord.PermissionOverwrite(
            view_channel=owner_view,
            send_messages=owner_send,
            read_message_history=owner_view,
            attach_files=owner_send,
            embed_links=owner_send,
        )

    for member in extra_members:
        if owner is not None and member.id == owner.id:
            continue
        set_member(member, is_staff_member=is_staff_level(get_staff_level(member, config)))

    return overwrites


def member_has_level(member: discord.Member | None, level: StaffLevel, config: Config) -> bool:
    return get_staff_level(member, config) >= level


def allowed_transcript_view_level(section: TicketSection) -> StaffLevel:
    if section in {TicketSection.PGT, TicketSection.UGT}:
        return StaffLevel.TRIAL_MOD
    if section == TicketSection.APPEAL:
        return StaffLevel.SUPERVISOR
    return StaffLevel.LEAGUE_MANAGER


def default_transcript_access(section: TicketSection) -> dict[str, bool]:
    access = {
        "owner": True,
        "trial_mod": False,
        "mod": False,
        "supervisor": False,
        "league_manager": False,
    }
    if section in {TicketSection.PGT, TicketSection.UGT}:
        access.update(
            {
                "trial_mod": True,
                "mod": True,
                "supervisor": True,
                "league_manager": True,
            }
        )
    elif section == TicketSection.APPEAL:
        access.update(
            {
                "supervisor": True,
                "league_manager": True,
            }
        )
    else:
        access["league_manager"] = True
    return access


def normalize_transcript_access(section: TicketSection, payload: dict[str, bool] | None) -> dict[str, bool]:
    access = default_transcript_access(section)
    if isinstance(payload, dict):
        for key in access:
            if key in payload:
                access[key] = bool(payload[key])
    return access


def transcript_access_key_for_level(level: StaffLevel) -> str | None:
    return {
        StaffLevel.TRIAL_MOD: "trial_mod",
        StaffLevel.MOD: "mod",
        StaffLevel.SUPERVISOR: "supervisor",
        StaffLevel.LEAGUE_MANAGER: "league_manager",
    }.get(level)


def summarize_staff_roles(config: Config) -> Iterable[tuple[str, int]]:
    return (
        ("Trial Mods", config.trial_mod_role_id),
        ("Mods", config.mod_role_id),
        ("Supervisors", config.supervisor_role_id),
        ("League Managers", config.league_manager_role_id),
    )
