from config.settings import QUEUE_NAME_STORE_CART

from nzovu import PostMessageParams

from ..sdk import invoke


async def process_cart(cart_id, cart, client):
    response = await invoke(client.post_message, PostMessageParams(cart_id, cart.model_dump(), QUEUE_NAME_STORE_CART))
    if not response.to_proto().success:
        raise RuntimeError("Server did not accept cart")
    return response
