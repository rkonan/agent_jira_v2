import json
import re

from models import Ticket, TicketClassification
from prompts import CLASSIFIER_PROMPT


def _extract_json(text: str) -> dict:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return {}
    return json.loads(match.group(0))





def classify_ticket(ticket: Ticket, entities, config, call_ollama_fn) -> TicketClassification:
    enriched_text = (
        f"{ticket.ticket_text}\n\n"
        f"Entités extraites:\n"
        f"- portfolio_codes: {entities.portfolio_codes}\n"
        f"- fund_names: {entities.fund_names}\n"
        f"- valuation_dates: {entities.valuation_dates}\n"
        f"- raw_hints: {entities.raw_hints}\n"
    )

    messages = [
        {"role": "system", "content": CLASSIFIER_PROMPT},
        {"role": "user", "content": enriched_text},
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

    return TicketClassification(
        request_type=data.get("request_type", "unknown_request"),
        issue_family=data.get("issue_family", "unknown"),
        missing_information=data.get("missing_information", []) or [],
        confidence=data.get("confidence", "medium"),
        reason=data.get("reason", ""),
    )
