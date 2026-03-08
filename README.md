# Agent Jira V2

Version avec :
- mode `backtest`
- mode `server`
- sauvegarde batch du backtest
- métriques automatiques
- envoi Outlook en mode serveur

## Installation

```bash
pip install -r requirements.txt
```

## Backtest

```bash
python app.py --mode backtest --backtest-input-file jira_backtest.jsonl --debug
```

## Backtest limité

```bash
python app.py --mode backtest --backtest-input-file jira_backtest.jsonl --max-tickets 10 --debug
```

## Server

```bash
python app.py --mode server --rss-url "https://ton-jira/rss" --send-mail --outlook-recipient "toi@entreprise.com"
```
