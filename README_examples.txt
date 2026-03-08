Exemples de fichiers pour tester agent_jira_v2.

1. jira_backtest_example.jsonl : tickets Jira exemples pour le mode backtest.
2. rag_index_example.jsonl : tickets historiques exemples pour le RAG.

Exemple de lancement backtest :
python app.py --mode backtest --backtest-input-file jira_backtest_example.jsonl --portfolio-reference-path portfolio_reference.csv --use-rag --rag-index-path rag_index_example.jsonl --debug
