import pytest
from unittest.mock import Mock


@pytest.fixture(scope="session")
def mock_nzovu_client():
    """Session-scoped mock Nzovu client."""
    client = Mock()

    # Mock common methods
    client.post_message = Mock()
    client.get_next_message = Mock()
    client.acknowledge_message = Mock()
    client.create_queue = Mock()
    client.get_queue = Mock()
    client.list_queues = Mock()
    client.delete_queue = Mock()
    client.close = Mock()

    return client


@pytest.fixture(autouse=True)
def reset_client_cache():
    """Reset the global client cache before each test."""
    from api.routes import store

    store.client_cache = None
    yield
    store.client_cache = None
