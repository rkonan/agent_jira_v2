import requests


def call_ollama(messages, model_name: str, ollama_url: str, keep_alive: str, tools=None, debug: bool = False):
    payload = {
        "model": model_name,
        "messages": messages,
        "stream": False,
        "keep_alive": keep_alive,
    }
    if tools:
        payload["tools"] = tools

    if debug:
        import json
        print("OLLAMA REQUEST:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))

    response = requests.post(ollama_url, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()

    if debug:
        import json
        print("OLLAMA RESPONSE:")
        print(json.dumps(data, indent=2, ensure_ascii=False))

    return data
