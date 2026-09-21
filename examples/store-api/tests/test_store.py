import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, MagicMock
from api.main import app
from api.models.request_models import CartItems, Item


@pytest.fixture
def mock_nzovu_client():
    """Mock NzovuClient for testing."""
    with patch("api.routes.store.NzovuClient") as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


class TestStoreAPI:
    """Test suite for Store API endpoints."""

    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/store/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "service": "store-api"}

    def test_post_cart_items_success(self, client, mock_nzovu_client):
        """Test successful cart submission."""
        # Mock the post_message response
        mock_response = Mock()
        mock_response.to_dict.return_value = {"message_id": "test-123", "status": "queued"}
        mock_nzovu_client.post_message.return_value = mock_response

        cart_data = {
            "items": [
                {"name": "Potato", "quantity": 1, "price": 10.0},
                {"name": "Banana", "quantity": 2, "price": 15.0},
            ]
        }

        with patch("api.routes.store.get_nzovu_client", return_value=mock_nzovu_client):
            response = client.post("/store/cart?cart_id=test-cart-123", json=cart_data)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "queued successfully" in data["message"]

    def test_post_cart_items_validation_error(self, client):
        """Test cart submission with invalid data."""
        invalid_cart = {"items": [{"name": "Potato", "quantity": "invalid", "price": 10.0}]}  # Invalid quantity type

        response = client.post("/store/cart", json=invalid_cart)
        assert response.status_code == 422  # Validation error

    def test_get_queue_stats_success(self, client, mock_nzovu_client):
        """Test getting queue statistics."""
        mock_response = Mock()
        mock_response.to_dict.return_value = {
            "queue": {
                "name": "store-cart",
                "message_count": 10,
                "pending_message_count": 5,
            }
        }
        mock_nzovu_client.get_queue.return_value = mock_response

        with patch("api.routes.store.get_nzovu_client", return_value=mock_nzovu_client):
            response = client.get("/store/queue/store-cart/stats")

        assert response.status_code == 200
        data = response.json()
        assert data["queue_name"] == "store-cart"
        assert "stats" in data

    def test_get_queue_stats_not_found(self, client, mock_nzovu_client):
        """Test getting stats for non-existent queue."""
        mock_nzovu_client.get_queue.side_effect = Exception("Queue not found")

        with patch("api.routes.store.get_nzovu_client", return_value=mock_nzovu_client):
            response = client.get("/store/queue/non-existent/stats")

        assert response.status_code == 404

    def test_list_queues_success(self, client, mock_nzovu_client):
        """Test listing queues."""
        mock_response = Mock()
        mock_response.to_dict.return_value = {
            "queues": [
                {"name": "store-cart", "message_count": 10},
                {"name": "checkout-cart", "message_count": 5},
            ],
            "total_count": 2,
        }
        mock_nzovu_client.list_queues.return_value = mock_response

        with patch("api.routes.store.get_nzovu_client", return_value=mock_nzovu_client):
            response = client.get("/store/queues")

        assert response.status_code == 200
        data = response.json()
        assert len(data["queues"]) == 2
        assert data["total_count"] == 2

    def test_list_queues_with_prefix(self, client, mock_nzovu_client):
        """Test listing queues with prefix filter."""
        mock_response = Mock()
        mock_response.to_dict.return_value = {
            "queues": [{"name": "store-cart", "message_count": 10}],
            "total_count": 1,
        }
        mock_nzovu_client.list_queues.return_value = mock_response

        with patch("api.routes.store.get_nzovu_client", return_value=mock_nzovu_client):
            response = client.get("/store/queues?prefix=store")

        assert response.status_code == 200
        data = response.json()
        assert len(data["queues"]) == 1
        mock_nzovu_client.list_queues.assert_called_once_with(prefix="store", limit=100)

    def test_get_pending_messages_count(self, client, mock_nzovu_client):
        """Test getting pending message count."""
        mock_response = Mock()
        mock_response.to_dict.return_value = {
            "queue": {
                "name": "store-cart",
                "message_count": 20,
                "pending_message_count": 8,
            }
        }
        mock_nzovu_client.get_queue.return_value = mock_response

        with patch("api.routes.store.get_nzovu_client", return_value=mock_nzovu_client):
            response = client.get("/store/queue/store-cart/messages/pending")

        assert response.status_code == 200
        data = response.json()
        assert data["queue_name"] == "store-cart"
        assert data["pending_messages"] == 8
        assert data["total_messages"] == 20

    def test_delete_queue_success(self, client, mock_nzovu_client):
        """Test deleting a queue."""
        mock_response = Mock()
        mock_response.to_dict.return_value = {"success": True}
        mock_nzovu_client.delete_queue.return_value = mock_response

        with patch("api.routes.store.get_nzovu_client", return_value=mock_nzovu_client):
            response = client.delete("/store/queue/test-queue")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "deleted successfully" in data["message"]
        mock_nzovu_client.delete_queue.assert_called_once_with(name="test-queue")
