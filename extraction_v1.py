import re
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime

PORTFOLIO_PATTERNS = [r"\bPF[0-9]{3,6}\b"]
GP_CODE_PATTERNS = [r"\bGP[-_ ]?[A-Z0-9]{2,12}\b"]
ISIN_PATTERNS = [r"\b[A-Z]{2}[A-Z0-9]{10}\b"]
DATE_PATTERNS = [r"\b\d{4}-\d{2}-\d{2}\b", r"\b\d{2}/\d{2}/\d{4}\b"]
AMOUNT_PATTERNS = [r"\b\d{1,3}(?:[ ,.]\d{3})+(?:[.,]\d{2})?\b"]
CURRENCY_PATTERNS = [r"\bEUR\b", r"\bUSD\b"]

@dataclass
class ExtractionField:
    value: Optional[Any]
    confidence: float
    source: Optional[str]
    candidates: Optional[List[Any]]

@dataclass
class ExtractionResult:
    portfolio: ExtractionField
    gp_code: ExtractionField
    isin: ExtractionField
    nav_date: ExtractionField
    amount: ExtractionField
    currency: ExtractionField

    def to_dict(self):
        return asdict(self)

def regex_find_all(text, patterns):
    matches = []
    for p in patterns:
        matches.extend(re.findall(p, text, flags=re.IGNORECASE))
    return list(dict.fromkeys(matches))

def choose_first(candidates):
    if not candidates:
        return ExtractionField(None, 0, None, [])
    return ExtractionField(candidates[0], 0.8, "regex", candidates)

def parse_date(date_str):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except:
            pass
    return None

def parse_amount(raw):
    raw = raw.replace(" ", "").replace(",", ".")
    try:
        val = float(raw)
        if val < 100:
            return None
        return val
    except:
        return None

def extract_entities(text):

    portfolio = choose_first(regex_find_all(text, PORTFOLIO_PATTERNS))
    gp_code = choose_first(regex_find_all(text, GP_CODE_PATTERNS))
    isin = choose_first(regex_find_all(text, ISIN_PATTERNS))

    date_candidates = regex_find_all(text, DATE_PATTERNS)
    date_val = parse_date(date_candidates[0]) if date_candidates else None
    nav_date = ExtractionField(date_val, 0.8 if date_val else 0, "regex", date_candidates)

    amount_candidates = regex_find_all(text, AMOUNT_PATTERNS)
    amount_val = parse_amount(amount_candidates[0]) if amount_candidates else None
    amount = ExtractionField(amount_val, 0.7 if amount_val else 0, "regex", amount_candidates)

    currency = choose_first(regex_find_all(text, CURRENCY_PATTERNS))

    return ExtractionResult(
        portfolio,
        gp_code,
        isin,
        nav_date,
        amount,
        currency
    )