from agent import run_analysis_agent
from classifier import classify_ticket
import config
from jira_source import load_jsonl_tickets
from llm import call_ollama
from metrics import compute_backtest_metrics, save_metrics
from rag import retrieve_similar_tickets
from report_builder import build_level1_summary
from scope_filter import load_portfolio_reference, run_scope_filter
from storage import save_jsonl
from datetime import datetime

def run_backtest(config):
    portfolio_reference = load_portfolio_reference(config.portfolio_reference_path)
    results = []
    batch_buffer = []

    for i, ticket in enumerate(load_jsonl_tickets(config.backtest_input_file), start=1):
        entities, scope_decision = run_scope_filter(
            ticket=ticket,
            config=config,
            call_ollama_fn=call_ollama,
            portfolio_reference=portfolio_reference,
        )

        if not scope_decision.in_scope:
            row = {
                "ticket_id": ticket.ticket_id,
                "status": "out_of_scope",
                "scope": scope_decision.scope,
                "reason": scope_decision.reason,
            }
            results.append(row)
            batch_buffer.append(row)
        else:
            classification = classify_ticket(
                ticket=ticket,
                entities=entities,
                config=config,
                call_ollama_fn=call_ollama,
            )

            if classification.request_type != "analysis_request":
                row = {
                    "ticket_id": ticket.ticket_id,
                    "status": classification.request_type,
                    "scope": scope_decision.scope,
                    "issue_family": classification.issue_family,
                    "missing_information": classification.missing_information,
                    "reason": classification.reason,
                }
                results.append(row)
                batch_buffer.append(row)
            else:
                similar_tickets = retrieve_similar_tickets(ticket, classification.issue_family, config)

                report = run_analysis_agent(
                    ticket=ticket,
                    scope_decision=scope_decision,
                    entities=entities,
                    classification=classification,
                    similar_tickets=similar_tickets,
                    config=config,
                )

                row = report.to_dict()
                row["scope_decision"] = scope_decision.to_dict()
                row["entities"] = entities.to_dict()
                row["classification"] = classification.to_dict()
                row["mail_preview"] = build_level1_summary(report)
                results.append(row)
                batch_buffer.append(row)

        if len(batch_buffer) >= config.backtest_batch_size:
            save_jsonl(config.backtest_output_file, batch_buffer, append=True)
            batch_buffer = []

        if config.max_tickets and i >= config.max_tickets:
            break
    time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    output_file = config.backtest_output_file.replace(".jsonl", f"_{time_stamp}.jsonl")
    metrics_file = config.backtest_output_file.replace(".jsonl", f"_{time_stamp}_metrics.json")

    if batch_buffer:
        save_jsonl(output_file, batch_buffer, append=True)

    metrics = compute_backtest_metrics(results)
    metrics_path = metrics_file
    save_metrics(metrics_path, metrics)

    print(f"Résultats écrits dans {output_file}")
    print(f"Métriques écrites dans {metrics_path}")
