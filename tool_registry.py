TOOLS_BY_FAMILY = {
    "nav_issue": ["extract_nav", "check_valuation_issue"],
    "performance_issue": ["compute_pnl", "extract_nav"],
    "oid_issue": ["check_oid_issue", "extract_nav"],
    "rappro_break": ["check_rappro_break", "extract_nav", "compute_pnl"],
    "pricing_issue": ["check_pricing_issue", "extract_nav"],
    "positions_issue": ["check_positions_issue", "extract_nav"],
    "cash_flow_issue": ["check_cash_flow_issue"],
    "valuation_gap": ["check_valuation_issue", "extract_nav", "compute_pnl"],
    "unknown": ["check_valuation_issue"],
}


ALL_OLLAMA_TOOLS = {
    "extract_nav": {
        "type": "function",
        "function": {
            "name": "extract_nav",
            "description": "Extrait la NAV pour un portefeuille et une date",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio": {"type": "string"},
                    "nav_date": {"type": "string"},
                },
                "required": ["portfolio", "nav_date"],
            },
        },
    },
    "compute_pnl": {
        "type": "function",
        "function": {
            "name": "compute_pnl",
            "description": "Calcule le PNL pour un portefeuille et une date",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio": {"type": "string"},
                    "nav_date": {"type": "string"},
                },
                "required": ["portfolio", "nav_date"],
            },
        },
    },
    "check_valuation_issue": {
        "type": "function",
        "function": {
            "name": "check_valuation_issue",
            "description": "Analyse un écart de valorisation pour un portefeuille et une date",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio": {"type": "string"},
                    "nav_date": {"type": "string"},
                },
                "required": ["portfolio", "nav_date"],
            },
        },
    },
    "check_oid_issue": {
        "type": "function",
        "function": {
            "name": "check_oid_issue",
            "description": "Analyse un problème de calcul OID",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio": {"type": "string"},
                    "nav_date": {"type": "string"},
                    "oid": {"type": "string"},
                },
                "required": ["portfolio"],
            },
        },
    },
    "check_rappro_break": {
        "type": "function",
        "function": {
            "name": "check_rappro_break",
            "description": "Analyse un déséquilibre de rapprochement",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio": {"type": "string"},
                    "nav_date": {"type": "string"},
                },
                "required": ["portfolio"],
            },
        },
    },
    "check_pricing_issue": {
        "type": "function",
        "function": {
            "name": "check_pricing_issue",
            "description": "Analyse un problème de pricing",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio": {"type": "string"},
                    "nav_date": {"type": "string"},
                },
                "required": ["portfolio"],
            },
        },
    },
    "check_positions_issue": {
        "type": "function",
        "function": {
            "name": "check_positions_issue",
            "description": "Analyse une incohérence de positions",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio": {"type": "string"},
                    "nav_date": {"type": "string"},
                },
                "required": ["portfolio"],
            },
        },
    },
    "check_cash_flow_issue": {
        "type": "function",
        "function": {
            "name": "check_cash_flow_issue",
            "description": "Analyse une anomalie de cash flow",
            "parameters": {
                "type": "object",
                "properties": {
                    "portfolio": {"type": "string"},
                    "nav_date": {"type": "string"},
                },
                "required": ["portfolio"],
            },
        },
    },
}


def get_allowed_tools(issue_family: str):
    allowed_names = TOOLS_BY_FAMILY.get(issue_family, TOOLS_BY_FAMILY["unknown"])
    return [ALL_OLLAMA_TOOLS[name] for name in allowed_names if name in ALL_OLLAMA_TOOLS], allowed_names
