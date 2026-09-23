from unittest.mock import AsyncMock, Mock

import pytest

from nzovu import AsyncNzovuClient, NzovuClient, ResponseWrapper
from nzovu.api.queueservice.v1 import request_response_pb2 as rpc


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def sdk(request):
    client = Mock(spec=AsyncNzovuClient if request.param else NzovuClient)
    client.asynchronous = request.param
    factory = AsyncMock if request.param else Mock
    for method, response in {
        "create_queue": rpc.CreateQueueResponse(success=True),
        "post_message": rpc.PostMessageResponse(success=True),
        "acknowledge_message": rpc.AcknowledgeMessageResponse(success=True),
        "list_queues": rpc.ListQueuesResponse(next_page_token="cursor"),
        "get_queue_state": rpc.GetQueueStateResponse(state_counts={"PENDING": 3, "RUNNING": 1}),
        "delete_queue": rpc.DeleteQueueResponse(success=True),
    }.items():
        setattr(client, method, factory(return_value=ResponseWrapper(response)))
    client.close = factory()
    client.stop_heartbeat = factory()
    client.get_active_heartbeat_count.return_value = 0
    return client
