import runpy
from pathlib import Path
from unittest.mock import Mock

import pytest

from nzovu import ResponseWrapper
from nzovu.api.queueservice.v1.request_response_pb2 import PostMessagesBulkResponse


def test_bulk_example_uses_distinct_ids_and_proto_counts(capsys):
    demo = runpy.run_path(str(Path(__file__).resolve().parents[2] / "bulk.py"))["demonstrate"]
    client = Mock()
    client.post_messages_bulk.return_value = ResponseWrapper(PostMessagesBulkResponse(successful_count=3))
    result = demo(client, "orders")
    assert len(result) == 2
    calls = client.post_messages_bulk.call_args_list
    ids = [{message.message_id for message in call.args[1]} for call in calls]
    assert len(ids[0]) == 3 and ids[0].isdisjoint(ids[1])
    assert "3 accepted" in capsys.readouterr().out


def test_bulk_example_rejects_invalid_tls_setting(monkeypatch):
    main = runpy.run_path(str(Path(__file__).resolve().parents[2] / "bulk.py"))["main"]
    factory = Mock()
    monkeypatch.setitem(main.__globals__, "NzovuClient", factory)
    monkeypatch.setenv("NZOVU_TLS_ENABLED", "typo")
    with pytest.raises(ValueError, match="must be true or false"):
        main()
    factory.assert_not_called()


@pytest.mark.parametrize("tls", ["true", "false"])
def test_bulk_example_closes_after_failed_post(monkeypatch, tls):
    main = runpy.run_path(str(Path(__file__).resolve().parents[2] / "bulk.py"))["main"]
    factory = Mock()
    factory.return_value.post_messages_bulk.side_effect = RuntimeError("post rejected")
    monkeypatch.setitem(main.__globals__, "NzovuClient", factory)
    monkeypatch.setenv("NZOVU_TLS_ENABLED", tls)
    with pytest.raises(RuntimeError, match="post rejected"):
        main()
    assert factory.call_args.kwargs["use_tls"] == (tls == "true")
    factory.return_value.close.assert_called_once()
