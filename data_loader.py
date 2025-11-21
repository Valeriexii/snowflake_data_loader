import os
import logging
from datetime import datetime, timezone

import requests

# -------------------------------------------------------------------
# Configuration (environment variables with defaults)
# -------------------------------------------------------------------
BASE_URL = os.getenv("MYSHOP_BASE_URL", "https://myshop.com")
SOURCE = os.getenv("MYSHOP_SOURCE", "myshop_api")
DATABASE = os.getenv("SNOWFLAKE_DATABASE", "RAW")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "ECOMMERCE")

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("myshop_loader")

# This is implemented by the company
def load_to_snowflake(schema, records, database, schema_name, table):
    return {"records_loaded": len(records), "records_skipped": 0}

# -------------------------------------------------------------------
# Custom error for API/network problems
# -------------------------------------------------------------------
class APIError(Exception):
    pass
    """Raised when a network problem occurs while calling the API."""

# -------------------------------------------------------------------
# 1. Fetch all pages from a paginated endpoint
# -------------------------------------------------------------------
def fetch_all(endpoint: str):
    """
    Fetch all records from a paginated endpoint:
    - Handles pagination using page number and total_pages.
    - Only raises APIError for network-related issues (timeouts, 500s, etc.).
    """
    full_url = f"{BASE_URL.rstrip('/')}{endpoint}"
    logger.info(f"Fetching data from {full_url}")
    records = []
    page = 1
    total_pages = 1
    
    while page <= total_pages:
        try:
            resp = requests.get(
                full_url,
                params={"page": page, "per_page": 100},
                timeout=10,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            raise APIError(f"Network error while fetching {full_url}: {e}") from e

        payload = resp.json()
        data = payload.get("data", [])
        records.extend(data)

        pagination = payload.get("pagination", {})
        total_pages = pagination.get("total_pages", 1)

        page += 1

    logger.info(f"Fetched {len(records)} records from {full_url}")
    return records

# -------------------------------------------------------------------
# 2. Metadata
# -------------------------------------------------------------------
def add_metadata(records):
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for r in records:
        r["_loaded_at"] = loaded_at
        r["_source"] = SOURCE
    return records

# -------------------------------------------------------------------
# 3. Simple transforms per entity
# -------------------------------------------------------------------
def transform_customers(records):
    """
    Flatten the nested address object and select the columns
    we want to load into Snowflake.
    """
    transformed = []
    for r in records:
        addr = r.get("address", {}) or {}
        transformed.append(
            {
                "id": r.get("id"),
                "email": r.get("email"),
                "first_name": r.get("first_name"),
                "last_name": r.get("last_name"),
                "phone": r.get("phone"),
                "street": addr.get("street"),
                "city": addr.get("city"),
                "state": addr.get("state"),
                "zip_code": addr.get("zip_code"),
                "country": addr.get("country"),
                "created_at": r.get("created_at"),
                "updated_at": r.get("updated_at"),
            }
        )
    return transformed


def transform_orders(records):
    return [
        {
            "id": r.get("id"),
            "customer_id": r.get("customer_id"),
            "order_number": r.get("order_number"),
            "status": r.get("status"),
            "total_amount": r.get("total_amount"),
            "currency": r.get("currency"),
            "order_date": r.get("order_date"),
            "shipped_date": r.get("shipped_date"),
            "delivered_date": r.get("delivered_date"),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
        }
        for r in records
    ]


def transform_order_line_items(records):
    return [
        {
            "id": r.get("id"),
            "order_id": r.get("order_id"),
            "product_id": r.get("product_id"),
            "product_name": r.get("product_name"),
            "quantity": r.get("quantity"),
            "unit_price": r.get("unit_price"),
            "total_price": r.get("total_price"),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
        }
        for r in records
    ]

# -------------------------------------------------------------------
# 4. Snowflake table schemas
# -------------------------------------------------------------------
CUSTOMERS_SCHEMA = {
    "id": "STRING",
    "email": "STRING",
    "first_name": "STRING",
    "last_name": "STRING",
    "phone": "STRING",
    "street": "STRING",
    "city": "STRING",
    "state": "STRING",
    "zip_code": "STRING",
    "country": "STRING",
    "created_at": "TIMESTAMP_TZ",
    "updated_at": "TIMESTAMP_TZ",
    "_loaded_at": "TIMESTAMP_TZ",
    "_source": "STRING",
}

ORDERS_SCHEMA = {
    "id": "STRING",
    "customer_id": "STRING",
    "order_number": "STRING",
    "status": "STRING",
    "total_amount": "NUMBER",
    "currency": "STRING",
    "order_date": "TIMESTAMP_TZ",
    "shipped_date": "TIMESTAMP_TZ",
    "delivered_date": "TIMESTAMP_TZ",
    "created_at": "TIMESTAMP_TZ",
    "updated_at": "TIMESTAMP_TZ",
    "_loaded_at": "TIMESTAMP_TZ",
    "_source": "STRING",
}

ORDER_LINE_ITEMS_SCHEMA = {
    "id": "STRING",
    "order_id": "STRING",
    "product_id": "STRING",
    "product_name": "STRING",
    "quantity": "NUMBER",
    "unit_price": "NUMBER",
    "total_price": "NUMBER",
    "created_at": "TIMESTAMP_TZ",
    "updated_at": "TIMESTAMP_TZ",
    "_loaded_at": "TIMESTAMP_TZ",
    "_source": "STRING",
}

# -------------------------------------------------------------------
# 5. Main pipeline
# -------------------------------------------------------------------
def main():
    logger.info("Pipeline started")

    try:
        # --- Customers ---
        customers_raw = fetch_all("/api/customers")
        customers = transform_customers(customers_raw)
        customers = add_metadata(customers)
        logger.info("Loading customers into Snowflake")
        load_to_snowflake(
            CUSTOMERS_SCHEMA,
            customers,
            DATABASE,
            SCHEMA,
            "CUSTOMERS",
        )

        # --- Orders ---
        orders_raw = fetch_all("/api/orders")
        orders = transform_orders(orders_raw)
        orders = add_metadata(orders)
        logger.info("Loading orders into Snowflake")
        load_to_snowflake(
            ORDERS_SCHEMA,
            orders,
            DATABASE,
            SCHEMA,
            "ORDERS",
        )

        # --- Order Line Items ---
        items_raw = fetch_all("/api/order-line-items")
        items = transform_order_line_items(items_raw)
        items = add_metadata(items)
        logger.info("Loading order line items into Snowflake")
        load_to_snowflake(
            ORDER_LINE_ITEMS_SCHEMA,
            items,
            DATABASE,
            SCHEMA,
            "ORDER_LINE_ITEMS",
        )

    except APIError as e:
        # Network-related issues: fail fast with a clear message
        logger.error(f"Pipeline failed due to API error: {e}")
        raise
    except Exception as e:
        # Catch unexpected bugs so they are logged
        logger.error(f"Pipeline failed due to unexpected error: {e}")
        raise

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()