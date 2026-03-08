from models import AgentFinalReport


def build_level1_summary(report: AgentFinalReport) -> str:
    findings_lines = "\n".join(f"- {x}" for x in report.findings) or "- Aucun constat"
    checks_lines = "\n".join(f"- {x}" for x in report.checks_run) or "- Aucun check"
    key_data_lines = "\n".join(f"- {k} : {v}" for k, v in report.key_data.items()) or "- Aucune donnée clé"
    similar_lines = "\n".join(
        f"- {x['ticket_id']} : {x['summary']}" for x in report.similar_tickets
    ) if report.similar_tickets else "- Aucun"

    return f"""Analyse automatique niveau 1

Ticket : {report.ticket_id}
Scope : {report.scope}
Type de problème : {report.issue_type}

Résumé
{report.summary}

Constats
{findings_lines}

Données clés
{key_data_lines}

Checks exécutés
{checks_lines}

Confiance
{report.confidence}

Action recommandée
{report.recommended_action}

Tickets similaires
{similar_lines}
"""
