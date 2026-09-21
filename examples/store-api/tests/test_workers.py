import pytest
from unittest.mock import Mock, AsyncMock, patch
from api.workers.store_cart_worker import process_cart
from api.workers.queue_manager_worker import create_store_cart_queue, create_checkout_cart_queue
from api.models.request_models import CartItems, Item
from nzovu.utils import QueueType


@pytest.fixture
def mock_client():
    """Mock NzovuClient."""
    return Mock()


@pytest.fixture
def sample_cart():
    """Sample cart for testing."""
    items = [
        Item(name="Potato", quantity=1, price=10.0),
        Item(name="Banana", quantity=2, price=15.0),
    ]
    return CartItems(items=items)


class TestStoreCartWorker:
    """Test suite for store cart worker."""

    @pytest.mark.asyncio
    async def test_process_cart_success(self, mock_client, sample_cart):
        """Test successful cart processing."""
        mock_response = Mock()
        mock_response.to_dict.return_value = {"message_id": "test-123", "status": "queued"}
        mock_client.post_message.return_value = mock_response

        await process_cart("cart-123", sample_cart, mock_client)

        # Verify post_message was called
        assert mock_client.post_message.called
        call_args = mock_client.post_message.call_args
        params = call_args.kwargs["msg_params"]
        assert params.queue_name == "store-cart"
        assert "items" in params.data

    @pytest.mark.asyncio
    async def test_process_cart_with_error(self, mock_client, sample_cart, caplog):
        """Test cart processing with error."""
        mock_client.post_message.side_effect = Exception("Connection failed")

        # Should not raise exception, but log error
        await process_cart("cart-456", sample_cart, mock_client)

        # Check error was logged
        assert "Error occurred in process_card" in caplog.text


class TestQueueManagerWorker:
    """Test suite for queue manager workers."""

    @pytest.mark.asyncio
    async def test_create_store_cart_queue_success(self, mock_client):
        """Test successful store cart queue creation."""
        mock_response = Mock()
        mock_response.to_dict.return_value = {"queue": {"name": "store-cart"}}
        mock_client.create_queue.return_value = mock_response

        await create_store_cart_queue(mock_client)

        # Verify create_queue was called with correct parameters
        assert mock_client.create_queue.called
        call_args = mock_client.create_queue.call_args
        assert call_args.kwargs["name"] == "store-cart"

        options = call_args.kwargs["options"]
        assert options.type == QueueType.SIMPLE
        assert options.max_attempts == 2

    @pytest.mark.asyncio
    async def test_create_store_cart_queue_error(self, mock_client, caplog):
        """Test store cart queue creation with error."""
        mock_client.create_queue.side_effect = Exception("Queue already exists")

        await create_store_cart_queue(mock_client)

        # Check error was logged
        assert "STORE ERROR" in caplog.text

    @pytest.mark.asyncio
    async def test_create_checkout_cart_queue_success(self, mock_client):
        """Test successful checkout cart queue creation."""
        mock_response = Mock()
        mock_response.to_proto.return_value = Mock()
        mock_client.create_queue.return_value = mock_response

        await create_checkout_cart_queue(mock_client)

        # Verify create_queue was called with correct parameters
        assert mock_client.create_queue.called
        call_args = mock_client.create_queue.call_args
        assert call_args.kwargs["name"] == "checkout-cart"

        options = call_args.kwargs["options"]
        assert options.type == QueueType.EXCLUSIVE
        assert options.exclusivity_key == "checkout-worker-1"
        assert options.max_attempts == -1

    @pytest.mark.asyncio
    async def test_create_checkout_cart_queue_error(self, mock_client, caplog):
        """Test checkout cart queue creation with error."""
        mock_client.create_queue.side_effect = Exception("Invalid configuration")

        await create_checkout_cart_queue(mock_client)

        # Check error was logged
        assert "CHECKOUT ERROR" in caplog.text
