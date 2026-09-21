from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from typing import Optional
import os
from nzovu.client import NzovuClient
from nzovu.utils import TlsConfig
from ..models.request_models import CartItems
from ..models.response_models import CartResponse
from ..workers import store_cart_worker
from config.settings import NZOVU_HOST, NZOVU_PORT, QUEUE_NAME_STORE_CART, QUEUE_NAME_CHECKOUT_CART

router = APIRouter()

# This is a cache to store the client instance
client_cache = None


def get_nzovu_client():
    global client_cache
    if client_cache is None:
        try:
            # Check if TLS certs exist
            ca_path = "./certs/ca.crt"
            client_crt_path = "./certs/client.crt"
            client_key_path = "./certs/client.key"

            use_tls = all(os.path.exists(p) for p in [ca_path, client_crt_path, client_key_path])

            if use_tls:
                client_cache = NzovuClient(
                    host=NZOVU_HOST,
                    port=NZOVU_PORT,
                    use_tls=True,
                    tls_config=TlsConfig(
                        ca_path=ca_path, client_crt_path=client_crt_path, client_key_path=client_key_path
                    ),
                )
            else:
                # Use insecure connection for testing/development
                client_cache = NzovuClient(
                    host=NZOVU_HOST,
                    port=NZOVU_PORT,
                    use_tls=False,
                )
        except Exception as e:
            # Handle client initialization errors if necessary
            raise HTTPException(status_code=500, detail=str(e))
    return client_cache


@router.post("/cart", response_model=CartResponse)
async def post_cart_items(
    cart_id: str,
    cart: CartItems,
    background_tasks: BackgroundTasks,
    client: NzovuClient = Depends(get_nzovu_client),
):
    """
    Submit cart items for processing.

    This endpoint demonstrates:
    - Posting messages to a queue
    - Background task processing
    - Basic message queuing workflow
    """
    try:
        # Add the task to post the cart to the "store-cart" queue to be executed in the background
        background_tasks.add_task(store_cart_worker.process_cart, cart_id, cart, client)
        return CartResponse(
            status="success",
            message="Cart items queued successfully for processing",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to queue cart: {str(e)}")


@router.get("/queue/{queue_name}/stats")
async def get_queue_stats(queue_name: str, client: NzovuClient = Depends(get_nzovu_client)):
    """
    Get statistics for a specific queue.

    This endpoint demonstrates:
    - Queue inspection
    - Monitoring queue health
    """
    try:
        response = client.get_queue(name=queue_name)
        queue_data = response.to_dict()

        return {
            "queue_name": queue_name,
            "stats": queue_data,
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Queue not found: {str(e)}")


@router.get("/queues")
async def list_queues(
    prefix: Optional[str] = None,
    limit: int = 100,
    client: NzovuClient = Depends(get_nzovu_client),
):
    """
    List all queues or filter by prefix.

    This endpoint demonstrates:
    - Listing queues
    - Queue discovery
    """
    try:
        response = client.list_queues(prefix=prefix or "", limit=limit)
        queues_data = response.to_dict()

        return {
            "queues": queues_data.get("queues", []),
            "total_count": queues_data.get("total_count", 0),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list queues: {str(e)}")


@router.get("/queue/{queue_name}/messages/pending")
async def get_pending_messages_count(queue_name: str, client: NzovuClient = Depends(get_nzovu_client)):
    """
    Get count of pending messages in a queue.

    This endpoint demonstrates:
    - Queue metrics
    - Message monitoring
    """
    try:
        response = client.get_queue(name=queue_name)
        queue_data = response.to_dict()

        return {
            "queue_name": queue_name,
            "pending_messages": queue_data.get("queue", {}).get("pending_message_count", 0),
            "total_messages": queue_data.get("queue", {}).get("message_count", 0),
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Queue not found: {str(e)}")


@router.delete("/queue/{queue_name}")
async def delete_queue(queue_name: str, client: NzovuClient = Depends(get_nzovu_client)):
    """
    Delete a queue (use with caution).

    This endpoint demonstrates:
    - Queue lifecycle management
    - Queue deletion
    """
    try:
        response = client.delete_queue(name=queue_name)
        result = response.to_dict()

        return {
            "status": "success",
            "message": f"Queue '{queue_name}' deleted successfully",
            "result": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete queue: {str(e)}")


@router.get("/health")
async def health_check():
    """Basic health check endpoint."""
    return {"status": "healthy", "service": "store-api"}
