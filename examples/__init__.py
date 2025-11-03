"""Example datasets from Malloy"""

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

DATASETS = {
    "flights": f"{BASE_URL}/flights.parquet",
    "carriers": f"{BASE_URL}/carriers.parquet",
    "airports": f"{BASE_URL}/airports.parquet",
    "order_items": f"{BASE_URL}/order_items.parquet",
    "users": f"{BASE_URL}/users.parquet",
    "aircraft": f"{BASE_URL}/aircraft.parquet",
    "aircraft_models": f"{BASE_URL}/aircraft_models.parquet",
    "products": f"{BASE_URL}/products.parquet",
    "inventory_items": f"{BASE_URL}/inventory_items.parquet",
    "ga_sample": f"{BASE_URL}/ga_sample.parquet",  # Google Analytics sample with nested data
}
