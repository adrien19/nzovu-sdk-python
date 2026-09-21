from uuid import uuid4
import logging
from nzovu.client import NzovuClient
from nzovu.utils import PostMessageParams, PostMessageOptions
from ..models.request_models import CartItems
from config.settings import QUEUE_NAME_STORE_CART

# Initialize logging
logging.basicConfig(level=logging.INFO)


async def process_cart(cart_id: str, cart: CartItems, client: NzovuClient):
    try:
        # Business logic to process the cart
        # Here, we're simply queuing the cart for checkout
        cart_dict = cart.model_dump()
        params = PostMessageParams(
            message_id=cart_id,
            data=cart_dict,
            queue_name=QUEUE_NAME_STORE_CART,
            # options=PostMessageOptions(
            #     lease_duration="5s",
            #     max_attempts=2,
            # ),
        )

        post_msg_resp = client.post_message(msg_params=params).to_dict()
        logging.info(f"======= DONE POSTING MESSAGE TO STORE CART {post_msg_resp} =======")

    except Exception as e:
        logging.error(f"Error occurred in process_card: {e}")
