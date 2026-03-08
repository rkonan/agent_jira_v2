from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


@dataclass
class Ticket:
    ticket_id: str
    summary: str
    description: str
    comments: List[str] = field(default_factory=list)
    status: str = ""
    resolution: str = ""

    @property
    def ticket_text(self) -> str:
        comments_text = "\n".join(self.comments) if self.comments else ""
        return (
            f"Objet: {self.summary}\n\n"
            f"Description:\n{self.description}\n\n"
            f"Commentaires:\n{comments_text}"
        ).strip()


@dataclass
class ScopeDecision:
    in_scope: bool
    scope: str
    matched_portfolio_code: Optional[str] = None
    matched_fund_name: Optional[str] = None
    match_method: str = "unknown"
    confidence: str = "medium"
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TicketEntities:
    portfolio_codes: List[str] = field(default_factory=list)
    fund_names: List[str] = field(default_factory=list)
    valuation_dates: List[str] = field(default_factory=list)
    instrument_names: List[str] = field(default_factory=list)
    oid_values: List[str] = field(default_factory=list)
    raw_hints: List[str] = field(default_factory=list)
    confidence: str = "medium"
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TicketClassification:
    request_type: str
    issue_family: str
    missing_information: List[str] = field(default_factory=list)
    confidence: str = "medium"
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ToolResult:
    status: str
    is_terminal: bool
    issue_type: str
    summary: str
    findings: List[str] = field(default_factory=list)
    key_data: Dict[str, Any] = field(default_factory=dict)
    recommended_next_tools: List[str] = field(default_factory=list)
    recommended_action: str = ""
    missing_information: List[str] = field(default_factory=list)
    confidence: str = "medium"
    raw_details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AgentStep:
    tool_name: str
    arguments: Dict[str, Any]
    result: ToolResult

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "arguments": self.arguments,
            "result": self.result.to_dict(),
        }


@dataclass
class SimilarTicket:
    ticket_id: str
    issue_family: str
    summary: str
    resolution_category: str = ""
    score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AgentFinalReport:
    model: str
    ticket_id: str
    status: str
    scope: str
    issue_type: str
    summary: str
    findings: List[str] = field(default_factory=list)
    key_data: Dict[str, Any] = field(default_factory=dict)
    checks_run: List[str] = field(default_factory=list)
    recommended_action: str = ""
    confidence: str = "medium"
    missing_information: List[str] = field(default_factory=list)
    needs_human_action: bool = True
    total_steps: int = 0
    llm_model: str = ""
    elapsed_seconds: float = 0.0
    similar_tickets: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
