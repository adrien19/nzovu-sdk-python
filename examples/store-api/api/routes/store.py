import grpc
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from nzovu import RpcOperationError

from ..models.request_models import CartItems
from ..models.response_models import CartResponse
from ..sdk import invoke
from ..workers.store_cart_worker import process_cart

router = APIRouter()


def get_nzovu_client(request: Request):
    return request.app.state.nzovu


def rpc_error(error):
    codes = {
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.ALREADY_EXISTS: 409,
        grpc.StatusCode.INVALID_ARGUMENT: 422,
        grpc.StatusCode.UNAVAILABLE: 503,
        grpc.StatusCode.DEADLINE_EXCEEDED: 504,
    }
    return HTTPException(status_code=codes.get(error.code(), 502), detail=error.details())


@router.post("/cart", response_model=CartResponse)
async def post_cart_items(
    cart: CartItems, cart_id: str = Query(pattern=r"^[A-Za-z0-9_-]{1,200}$"), client=Depends(get_nzovu_client)
):
    try:
        await process_cart(cart_id, cart, client)
        return CartResponse(status="queued", message="Cart queued for processing", cart_id=cart_id)
    except RpcOperationError as error:
        raise rpc_error(error) from error
    except RuntimeError as error:
        raise HTTPException(status_code=502, detail=str(error)) from error


@router.get("/queue/{queue_name}/stats")
async def get_queue_stats(queue_name: str, client=Depends(get_nzovu_client)):
    try:
        response = await invoke(client.get_queue_state, queue_name)
        return {"queue_name": queue_name, "stats": response.to_dict()}
    except RpcOperationError as error:
        raise rpc_error(error) from error


@router.get("/queues")
async def list_queues(
    prefix: str = "", page_size: int = Query(100, ge=0, le=1000), page_token: str = "", client=Depends(get_nzovu_client)
):
    try:
        return (await invoke(client.list_queues, prefix=prefix, page_size=page_size, page_token=page_token)).to_dict()
    except RpcOperationError as error:
        raise rpc_error(error) from error


@router.get("/queue/{queue_name}/messages/pending")
async def get_pending_messages_count(queue_name: str, client=Depends(get_nzovu_client)):
    try:
        counts = (await invoke(client.get_queue_state, queue_name)).to_proto().state_counts
        return {
            "queue_name": queue_name,
            "pending_messages": counts.get("PENDING", 0),
            "total_messages": sum(counts.values()),
        }
    except RpcOperationError as error:
        raise rpc_error(error) from error


@router.delete("/queue/{queue_name}")
async def delete_queue(queue_name: str, client=Depends(get_nzovu_client)):
    try:
        return (await invoke(client.delete_queue, name=queue_name)).to_dict()
    except RpcOperationError as error:
        raise rpc_error(error) from error


@router.get("/health")
async def health_check(client=Depends(get_nzovu_client)):
    return {"status": "healthy", "service": "store-api", "active_heartbeats": client.get_active_heartbeat_count()}
