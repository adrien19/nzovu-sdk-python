"""Bulk modes using distinct IDs; run against an explicitly configured local server."""

import os
from uuid import uuid4

from nzovu import NzovuClient, PostMessageParams, TransactionMode


def demonstrate(client, queue):
    results = []
    for mode in TransactionMode:
        messages = [PostMessageParams(uuid4().hex, {"order_id": index}, queue) for index in range(3)]
        response = client.post_messages_bulk(queue, messages, transaction_mode=mode).to_proto()
        results.append(response)
        print(f"{mode.value}: {response.successful_count} accepted, {response.failed_count} rejected")
    return results


def main():
    tls = os.getenv("NZOVU_TLS_ENABLED", "true").lower()
    if tls not in {"true", "false"}:
        raise ValueError("NZOVU_TLS_ENABLED must be true or false")
    client = NzovuClient(
        os.getenv("NZOVU_HOST", "localhost"),
        port=int(os.getenv("NZOVU_PORT", "9000")),
        use_tls=tls == "true",
        api_key=os.getenv("NZOVU_API_KEY"),
        rpc_timeout=5,
    )
    try:
        queue = "bulk_" + uuid4().hex
        client.create_queue(queue)
        demonstrate(client, queue)
    finally:
        client.close()


if __name__ == "__main__":
    main()
