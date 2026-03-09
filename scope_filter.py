import csv
import json
import re
from typing import Dict, List

from models import ScopeDecision, Ticket, TicketEntities
from prompts import SCOPE_EXTRACTION_PROMPT


def load_portfolio_reference(path: str) -> List[Dict[str, str]]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def extract_entities_regex(ticket: Ticket) -> TicketEntities:
    text = ticket.ticket_text
    portfolio_codes = re.findall(r"\b[A-Z]{2,4}\d{2,}\b", text)
    dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", text)
    domain="peres" if any(ticket.labels and ("peres" in label.lower() or "hybrid" in label.lower()) for label in ticket.labels) else "unknown"
    
    return TicketEntities(
        portfolio_codes=list(dict.fromkeys(portfolio_codes)),
        valuation_dates=list(dict.fromkeys(dates)),
        scope=domain,
        confidence="high" if portfolio_codes else "low",
        reason="Extraction regex"
    )




def _extract_json(text: str) -> dict:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return {}
    return json.loads(match.group(0))


def extract_entities_llm(ticket: Ticket, config, call_ollama_fn) -> TicketEntities:
    messages = [
        {"role": "system", "content": SCOPE_EXTRACTION_PROMPT},
        {"role": "user", "content": ticket.ticket_text},
    ]
    response = call_ollama_fn(
        messages=messages,
        model_name=config.model_name,
        ollama_url=config.ollama_url,
        keep_alive=config.keep_alive,
        tools=None,
        debug=config.debug,
    )
    data = _extract_json(response["message"].get("content", ""))
    return TicketEntities(
        portfolio_codes=data.get("portfolio_codes", []) or [],
        fund_names=data.get("fund_names", []) or [],
        raw_hints=data.get("raw_hints", []) or [],
        confidence=data.get("confidence", "medium"),
        reason=data.get("reason", ""),
    )


def resolve_scope(entities: TicketEntities, portfolio_reference: List[Dict[str, str]]) -> ScopeDecision:
    
    if entities.scope == "peres":
        for code in entities.portfolio_codes:
                for row in portfolio_reference:
                    if row.get("portfolio_code", "").lower() == code.lower():
                    
                        return ScopeDecision(
                            in_scope=True,
                            scope=entities.scope,
                        matched_portfolio_code=row.get("portfolio_code"),
                        matched_fund_name=row.get("fund_name"),
                        match_method="exact_code",
                        confidence="high",
                        reason=f"Code portefeuille reconnu: {code}",
                    )


    for code in entities.portfolio_codes:
        for row in portfolio_reference:
            if row.get("portfolio_code", "").lower() == code.lower():
                scope = row.get("scope", "unknown").lower()
                return ScopeDecision(
                    in_scope=scope == "peres",
                    scope=scope,
                    matched_portfolio_code=row.get("portfolio_code"),
                    matched_fund_name=row.get("fund_name"),
                    match_method="exact_code",
                    confidence="high",
                    reason=f"Code portefeuille reconnu: {code}",
                )

    for name in entities.fund_names:
        for row in portfolio_reference:
            if row.get("fund_name", "").lower() == name.lower():
                scope = row.get("scope", "unknown").lower()
                return ScopeDecision(
                    in_scope=scope == "peres",
                    scope=scope,
                    matched_portfolio_code=row.get("portfolio_code"),
                    matched_fund_name=row.get("fund_name"),
                    match_method="exact_fund_name",
                    confidence="medium",
                    reason=f"Nom de fonds reconnu: {name}",
                )

    hints = [h.lower() for h in entities.raw_hints]
    for hint in hints:
        for row in portfolio_reference:
            aliases = [a.strip().lower() for a in (row.get("aliases") or "").split("|") if a.strip()]
            if hint in aliases:
                scope = row.get("scope", "unknown").lower()
                return ScopeDecision(
                    in_scope=scope == "peres",
                    scope=scope,
                    matched_portfolio_code=row.get("portfolio_code"),
                    matched_fund_name=row.get("fund_name"),
                    match_method="alias_match",
                    confidence="medium",
                    reason=f"Alias reconnu: {hint}",
                )

    return ScopeDecision(
        in_scope=False,
        scope="unknown",
        match_method="unresolved",
        confidence="low",
        reason="Impossible de déterminer le scope",
    )


def run_scope_filter(ticket: Ticket, config, call_ollama_fn, portfolio_reference: List[Dict[str, str]]):

    entities = extract_entities_regex(ticket)
    if not entities.portfolio_codes:
        llm_entities = extract_entities_llm(ticket, config, call_ollama_fn)
        entities.portfolio_codes = llm_entities.portfolio_codes
        entities.fund_names = llm_entities.fund_names
        entities.raw_hints = llm_entities.raw_hints
        entities.confidence = llm_entities.confidence
        entities.reason = llm_entities.reason

    scope_decision = resolve_scope(entities, portfolio_reference)
    return entities, scope_decision
