# snowflake_data_loader

- Extracts customers, orders, and order line items from the MyShop API, handling pagination using the API’s total_pages value.

- Adds simple transformations (e.g., flattening customer addresses) and metadata fields (_loaded_at, _source).

- Loads each dataset into Snowflake using the provided load_to_snowflake function with hard-coded schemas for clarity.

- Uses environment variables for configuration and minimal logging for visibility.

- Focuses on a clear, lightweight structure; production improvements could include retries, incremental loading, and validation.