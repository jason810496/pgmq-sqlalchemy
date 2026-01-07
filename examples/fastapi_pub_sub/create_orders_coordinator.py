import time
import logging
import os

import httpx


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

API_PORT = int(os.getenv("API_PORT", "8000"))
BASE_URL = f"http://localhost:{API_PORT}"


def wait_until_api_server_to_start() -> None:
    # Wait for the server to start
    start_time = time.time()
    max_wait = 30
    while True:
        try:
            response = httpx.get(f"{BASE_URL}/health", timeout=1)
            if response.status_code == 200:
                logger.info("API Server is ready!")
                return
        except Exception:
            time.sleep(0.05)
        logger.info("API Server is not ready...")

        if (time.time() - start_time) > max_wait:
            break

    raise RuntimeError("API server failed to start")


def create_order(order_num: int):
    """Helper function to create a single order."""
    order_data = {
        "customer_name": f"Customer {order_num}",
        "product_name": f"Product {order_num}",
        "quantity": order_num % 10 + 1,
        "price": 10.0 + (order_num % 50),
    }
    response = httpx.post(f"{BASE_URL}/orders", json=order_data, timeout=5)
    return (
        response.status_code == 201,
        response.json() if response.status_code == 201 else None,
    )


def create_orders_parallel(num_orders: int):
    import concurrent.futures

    # Create orders in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(create_order, i) for i in range(num_orders)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    # Check that all orders were created successfully
    successful_orders = sum(1 for success, _ in results if success)
    assert (
        successful_orders == num_orders
    ), f"Only {successful_orders}/{num_orders} orders were created"
    logger.info()
    logger.info("Create %d successful orders via API Server", successful_orders)


if __name__ == "__main__":
    wait_until_api_server_to_start()
    create_orders_parallel(num_orders=100)
