from models import ToolResult


def extract_nav(portfolio: str, nav_date: str) -> ToolResult:
    return ToolResult(
        status="ok",
        is_terminal=False,
        issue_type="nav_issue",
        summary=f"NAV récupérée pour {portfolio} au {nav_date}",
        findings=[f"NAV disponible pour {portfolio} au {nav_date}"],
        key_data={"portfolio": portfolio, "date": nav_date, "nav": 1250345.78, "currency": "EUR"},
        recommended_action="Comparer la NAV récupérée avec la valeur attendue.",
        confidence="high",
    )


def compute_pnl(portfolio: str, nav_date: str) -> ToolResult:
    return ToolResult(
        status="ok",
        is_terminal=False,
        issue_type="performance_issue",
        summary=f"PNL calculé pour {portfolio} au {nav_date}",
        findings=[f"PNL disponible pour {portfolio} au {nav_date}"],
        key_data={"portfolio": portfolio, "date": nav_date, "pnl": 23456.78, "currency": "EUR"},
        recommended_action="Comparer le PNL calculé avec la performance attendue.",
        confidence="high",
    )


def check_valuation_issue(portfolio: str, nav_date: str) -> ToolResult:
    return ToolResult(
        status="issue_found",
        is_terminal=False,
        issue_type="valuation_gap",
        summary=f"Ecart de valorisation détecté pour {portfolio} au {nav_date}",
        findings=[
            "Le ticket mentionne un écart de valorisation.",
            "Une vérification NAV et PNL est pertinente en niveau 1.",
        ],
        key_data={"portfolio": portfolio, "date": nav_date},
        recommended_next_tools=["extract_nav", "compute_pnl"],
        recommended_action="Contrôler la NAV et le PNL avant analyse plus poussée.",
        confidence="medium",
    )


def check_oid_issue(portfolio: str, nav_date: str = "", oid: str = "") -> ToolResult:
    return ToolResult(
        status="inconclusive" if not nav_date else "issue_found",
        is_terminal=False,
        issue_type="oid_issue",
        summary="Analyse OID réalisée",
        findings=["Vérification initiale du calcul OID effectuée"],
        key_data={"portfolio": portfolio, "date": nav_date, "oid": oid},
        recommended_action="Vérifier le paramétrage OID et la date de valorisation.",
        missing_information=[] if nav_date else ["valuation_date"],
        confidence="medium",
    )


def check_rappro_break(portfolio: str, nav_date: str = "") -> ToolResult:
    return ToolResult(
        status="issue_found",
        is_terminal=False,
        issue_type="rappro_break",
        summary="Déséquilibre de rapprochement détecté",
        findings=["Un écart de rapprochement a été détecté"],
        key_data={"portfolio": portfolio, "date": nav_date, "gap": 12450.22, "currency": "EUR"},
        recommended_action="Vérifier flux cash et positions valorisées.",
        confidence="medium",
    )


def check_pricing_issue(portfolio: str, nav_date: str = "") -> ToolResult:
    return ToolResult(
        status="issue_found",
        is_terminal=False,
        issue_type="pricing_issue",
        summary="Anomalie de pricing détectée",
        findings=["La source de prix semble incohérente"],
        key_data={"portfolio": portfolio, "date": nav_date},
        recommended_action="Contrôler la source de prix et le market data.",
        confidence="medium",
    )


def check_positions_issue(portfolio: str, nav_date: str = "") -> ToolResult:
    return ToolResult(
        status="issue_found",
        is_terminal=False,
        issue_type="positions_issue",
        summary="Incohérence de positions détectée",
        findings=["Une anomalie de positions est suspectée"],
        key_data={"portfolio": portfolio, "date": nav_date},
        recommended_action="Comparer les positions valorisées avec la source front.",
        confidence="medium",
    )


def check_cash_flow_issue(portfolio: str, nav_date: str = "") -> ToolResult:
    return ToolResult(
        status="issue_found",
        is_terminal=False,
        issue_type="cash_flow_issue",
        summary="Anomalie de cash flow détectée",
        findings=["Un flux cash semble manquant ou mal intégré"],
        key_data={"portfolio": portfolio, "date": nav_date},
        recommended_action="Vérifier l'intégration des cash flows.",
        confidence="medium",
    )


def run_check(tool_name: str, arguments: dict) -> ToolResult:
    if tool_name == "extract_nav":
        return extract_nav(**arguments)
    if tool_name == "compute_pnl":
        return compute_pnl(**arguments)
    if tool_name == "check_valuation_issue":
        return check_valuation_issue(**arguments)
    if tool_name == "check_oid_issue":
        return check_oid_issue(**arguments)
    if tool_name == "check_rappro_break":
        return check_rappro_break(**arguments)
    if tool_name == "check_pricing_issue":
        return check_pricing_issue(**arguments)
    if tool_name == "check_positions_issue":
        return check_positions_issue(**arguments)
    if tool_name == "check_cash_flow_issue":
        return check_cash_flow_issue(**arguments)

    return ToolResult(
        status="error",
        is_terminal=True,
        issue_type="unknown",
        summary=f"Tool inconnu: {tool_name}",
        findings=[f"Tool inconnu: {tool_name}"],
        recommended_action="Vérifier le registre de tools.",
        confidence="low",
    )
