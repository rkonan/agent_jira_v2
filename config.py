from dataclasses import dataclass


@dataclass
class AppConfig:
    model_name: str = "qwen2.5:3B"
    ollama_url: str = "http://127.0.0.1:11434/api/chat"
    keep_alive: str = "10m"
    max_steps: int = 4
    debug: bool = False

    portfolio_reference_path: str = "portfolio_reference.csv"

    use_rag: bool = False
    rag_index_path: str = "rag_index.jsonl"
    rag_top_k: int = 3

    rss_url: str = ""
    poll_interval_seconds: int = 300
    state_file: str = "processed_tickets.json"
    send_mail: bool = False
    outlook_recipient: str = ""

    backtest_input_file: str = ""
    backtest_output_file: str = "backtest_results.jsonl"
    backtest_batch_size: int = 20
    max_tickets: int | None = None
