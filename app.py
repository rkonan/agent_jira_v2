import argparse

from backtest_runner import run_backtest
from config import AppConfig
from server_runner import run_server


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backtest", "server"], required=True)
    parser.add_argument("--model-name", default="qwen2.5:3B")
    parser.add_argument("--ollama-url", default="http://127.0.0.1:11434/api/chat")
    parser.add_argument("--keep-alive", default="10m")
    parser.add_argument("--max-steps", type=int, default=4)
    parser.add_argument("--debug", action="store_true")

    parser.add_argument("--backtest-input-file", default="backtest_input/jira_backtest_example.jsonl")
    parser.add_argument("--backtest-output-file", default="backtest_output/backtest_results.jsonl")
    parser.add_argument("--backtest-batch-size", type=int, default=20)
    parser.add_argument("--max-tickets", type=int, default=None)

    parser.add_argument("--portfolio-reference-path", default="backtest_input/portfolio_reference.csv")

    parser.add_argument("--use-rag", action="store_true")
    parser.add_argument("--rag-index-path", default="rag_index.jsonl")
    parser.add_argument("--rag-top-k", type=int, default=3)

    parser.add_argument("--rss-url", default="")
    parser.add_argument("--poll-interval-seconds", type=int, default=300)
    parser.add_argument("--state-file", default="processed_tickets.json")

    parser.add_argument("--send-mail", action="store_true")
    parser.add_argument("--outlook-recipient", default="")

    return parser.parse_args()


def build_config(args):
    return AppConfig(
        model_name=args.model_name,
        ollama_url=args.ollama_url,
        keep_alive=args.keep_alive,
        max_steps=args.max_steps,
        debug=args.debug,
        backtest_input_file=args.backtest_input_file,
        backtest_output_file=args.backtest_output_file,
        backtest_batch_size=args.backtest_batch_size,
        max_tickets=args.max_tickets,
        portfolio_reference_path=args.portfolio_reference_path,
        use_rag=args.use_rag,
        rag_index_path=args.rag_index_path,
        rag_top_k=args.rag_top_k,
        rss_url=args.rss_url,
        poll_interval_seconds=args.poll_interval_seconds,
        state_file=args.state_file,
        send_mail=args.send_mail,
        outlook_recipient=args.outlook_recipient,
    )


def main():
    args = parse_args()
    config = build_config(args)

    if args.mode == "backtest":
        if not config.backtest_input_file:
            raise ValueError("Le mode backtest nécessite --backtest-input-file")
        run_backtest(config)
        return

    if args.mode == "server":
        if not config.rss_url:
            raise ValueError("Le mode server nécessite --rss-url")
        if config.send_mail and not config.outlook_recipient:
            raise ValueError("Si --send-mail est activé, il faut --outlook-recipient")
        run_server(config)
        return


if __name__ == "__main__":
    main()
