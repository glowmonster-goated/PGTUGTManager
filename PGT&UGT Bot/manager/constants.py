from __future__ import annotations

from enum import Enum, IntEnum

try:
    from enum import StrEnum
except ImportError:
    class StrEnum(str, Enum):
        pass


class TicketSection(StrEnum):
    APPEAL = "appeal"
    MANAGEMENT = "management"
    PGT = "pgt"
    UGT = "ugt"

    @property
    def label(self) -> str:
        return {
            TicketSection.APPEAL: "Appeal",
            TicketSection.MANAGEMENT: "Management",
            TicketSection.PGT: "PGT Support",
            TicketSection.UGT: "UGT Support",
        }[self]

    @property
    def prefix(self) -> str:
        return self.value


class TicketState(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    DELETED = "deleted"


class StaffLevel(IntEnum):
    NONE = 0
    TRIAL_MOD = 1
    MOD = 2
    SUPERVISOR = 3
    LEAGUE_MANAGER = 4


PANEL_CUSTOM_IDS = {
    TicketSection.APPEAL: "ticket:panel:appeal",
    TicketSection.MANAGEMENT: "ticket:panel:management",
    TicketSection.PGT: "ticket:panel:pgt",
    TicketSection.UGT: "ticket:panel:ugt",
}

CLOSE_BUTTON_ID = "ticket:close"
DELETE_BUTTON_ID = "ticket:delete"
MANAGEMENT_CONFIRM_ID = "ticket:management:confirm"
MANAGEMENT_CANCEL_ID = "ticket:management:cancel"
CLOSE_REQUEST_BUTTON_ID = "ticket:close-request"
