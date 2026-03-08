import time

from agent import run_analysis_agent
from classifier import classify_ticket
from jira_source import fetch_rss_tickets
from llm import call_ollama
from mail_sender import send_outlook_mail
from rag import retrieve_similar_tickets
from report_builder import build_level1_summary
from scope_filter import load_portfolio_reference, run_scope_filter
from storage import load_processed_ids, save_processed_ids


def run_server(config):
    portfolio_reference = load_portfolio_reference(config.portfolio_reference_path)
    processed_ids = load_processed_ids(config.state_file)

    while True:
        tickets = fetch_rss_tickets(config.rss_url)
        new_tickets = [t for t in tickets if t.ticket_id not in processed_ids]

        for ticket in new_tickets:
            entities, scope_decision = run_scope_filter(
                ticket=ticket,
                config=config,
                call_ollama_fn=call_ollama,
                portfolio_reference=portfolio_reference,
            )

            if not scope_decision.in_scope:
                processed_ids.add(ticket.ticket_id)
                save_processed_ids(config.state_file, processed_ids)
                continue

            classification = classify_ticket(
                ticket=ticket,
                entities=entities,
                config=config,
                call_ollama_fn=call_ollama,
            )

            if classification.request_type != "analysis_request":
                processed_ids.add(ticket.ticket_id)
                save_processed_ids(config.state_file, processed_ids)
                continue

            similar_tickets = retrieve_similar_tickets(ticket, classification.issue_family, config)

            report = run_analysis_agent(
                ticket=ticket,
                scope_decision=scope_decision,
                entities=entities,
                classification=classification,
                similar_tickets=similar_tickets,
                config=config,
            )

            if config.send_mail:
                send_outlook_mail(
                    subject=f"Analyse niveau 1 Jira {ticket.ticket_id}",
                    body=build_level1_summary(report),
                    to=config.outlook_recipient,
                )

            processed_ids.add(ticket.ticket_id)
            save_processed_ids(config.state_file, processed_ids)

        time.sleep(config.poll_interval_seconds)
