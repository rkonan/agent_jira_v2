import json
import time

from check_engine import run_check
from llm import call_ollama
from models import AgentFinalReport, AgentStep, Ticket
from prompts import AGENT_SYSTEM_PROMPT_TEMPLATE
from tool_registry import get_allowed_tools


def _build_agent_system_prompt(scope: str, issue_family: str, entities, similar_tickets, allowed_tool_names):
    allowed_tools_bullets = "\n".join([f"- {name}" for name in allowed_tool_names])
    similar_text = (
        "\n".join([f"- {t.ticket_id}: {t.summary} ({t.resolution_category})" for t in similar_tickets])
        if similar_tickets else "- Aucun ticket similaire"
    )
    return AGENT_SYSTEM_PROMPT_TEMPLATE.format(
        scope=scope,
        issue_family=issue_family,
        entities_json=json.dumps(entities.to_dict(), ensure_ascii=False),
        allowed_tools_bullets=allowed_tools_bullets,
        similar_tickets_text=similar_text,
    )


def consolidate_report(ticket: Ticket, scope: str, steps, similar_tickets, config, elapsed_seconds: float) -> AgentFinalReport:
    findings = []
    key_data = {}
    checks_run = []
    missing_information = []
    recommended_actions = []
    issue_types = []
    confidence_rank = {"low": 1, "medium": 2, "high": 3}
    max_confidence = "low"

    for step in steps:
        checks_run.append(step.tool_name)
        result = step.result
        findings.extend(result.findings)
        key_data.update(result.key_data)
        missing_information.extend(result.missing_information)
        if result.recommended_action:
            recommended_actions.append(result.recommended_action)
        if result.issue_type and result.issue_type != "unknown":
            issue_types.append(result.issue_type)
        if confidence_rank.get(result.confidence, 1) > confidence_rank.get(max_confidence, 1):
            max_confidence = result.confidence

    issue_type = issue_types[-1] if issue_types else "unknown"
    status = "need_human_review" if missing_information else "level_1_done"
    summary = "Analyse niveau 1 partielle, des informations manquent." if missing_information else "Analyse niveau 1 réalisée."

    return AgentFinalReport(
        ticket_id=ticket.ticket_id,
        status=status,
        scope=scope,
        issue_type=issue_type,
        summary=summary,
        findings=findings,
        key_data=key_data,
        checks_run=checks_run,
        recommended_action=recommended_actions[-1] if recommended_actions else "Analyse humaine requise.",
        confidence=max_confidence,
        missing_information=missing_information,
        needs_human_action=True,
        total_steps=len(steps),
        llm_model=config.model_name,
        elapsed_seconds=elapsed_seconds,
        similar_tickets=[t.to_dict() for t in similar_tickets],
    )


def run_analysis_agent(ticket: Ticket, scope_decision, entities, classification, similar_tickets, config) -> AgentFinalReport:
    start = time.perf_counter()

    allowed_tools, allowed_tool_names = get_allowed_tools(classification.issue_family)
    system_prompt = _build_agent_system_prompt(
        scope=scope_decision.scope,
        issue_family=classification.issue_family,
        entities=entities,
        similar_tickets=similar_tickets,
        allowed_tool_names=allowed_tool_names,
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": ticket.ticket_text},
    ]

    steps = []
    seen_calls = set()

    for _ in range(config.max_steps):
        response = call_ollama(
            messages=messages,
            tools=allowed_tools,
            model_name=config.model_name,
            ollama_url=config.ollama_url,
            keep_alive=config.keep_alive,
            debug=config.debug,
        )

        msg = response["message"]
        tool_calls = msg.get("tool_calls", [])

        if not tool_calls:
            break

        messages.append({
            "role": "assistant",
            "content": msg.get("content", ""),
            "tool_calls": tool_calls,
        })

        for tool_call in tool_calls:
            tool_name = tool_call["function"]["name"]
            arguments = tool_call["function"]["arguments"]

            signature = (tool_name, json.dumps(arguments, sort_keys=True, ensure_ascii=False))
            if signature in seen_calls:
                continue
            seen_calls.add(signature)

            result = run_check(tool_name, arguments)
            steps.append(AgentStep(tool_name=tool_name, arguments=arguments, result=result))

            messages.append({
                "role": "tool",
                "name": tool_name,
                "content": json.dumps(result.to_dict(), ensure_ascii=False),
            })

    elapsed = time.perf_counter() - start
    return consolidate_report(ticket, scope_decision.scope, steps, similar_tickets, config, elapsed)
