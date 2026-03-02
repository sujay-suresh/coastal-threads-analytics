"""
Generate synthetic data for Coastal Threads Analytics.

Creates realistic e-commerce data for a DTC fashion retailer:
- 30,000 customers
- 55,000 orders (power-law frequency)
- 95,000 order items across 200 SKUs in 8 categories
- 400,000 customer events (page views, email opens, ad clicks, organic visits)
- Stripe payments matching order totals

Loads all data into PostgreSQL schema: coastal_threads
"""

import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import hashlib
import warnings

warnings.filterwarnings("ignore")

fake = Faker()
Faker.seed(42)
np.random.seed(42)

# --- Config ---
DB_URL = "postgresql://portfolio:portfolio_dev@localhost:5432/portfolio"
SCHEMA = "coastal_threads"
N_CUSTOMERS = 30_000
N_ORDERS = 55_000
N_ORDER_ITEMS = 95_000
N_EVENTS = 400_000
N_PRODUCTS = 200

# Date range: 2 years of history
END_DATE = datetime(2025, 12, 31)
START_DATE = datetime(2024, 1, 1)

CATEGORIES = [
    "dresses", "tops", "bottoms", "outerwear",
    "shoes", "accessories", "swimwear", "activewear",
]

SUBCATEGORIES = {
    "dresses": ["maxi", "midi", "mini", "wrap", "shirt_dress"],
    "tops": ["blouse", "tank", "tee", "crop_top", "sweater"],
    "bottoms": ["jeans", "shorts", "skirt", "trousers", "leggings"],
    "outerwear": ["jacket", "coat", "blazer", "cardigan", "vest"],
    "shoes": ["sandals", "sneakers", "boots", "heels", "flats"],
    "accessories": ["bag", "hat", "scarf", "jewelry", "sunglasses"],
    "swimwear": ["bikini", "one_piece", "cover_up", "board_shorts"],
    "activewear": ["sports_bra", "yoga_pants", "tank", "hoodie", "shorts"],
}

# Price ranges by category
PRICE_RANGES = {
    "dresses": (55, 120),
    "tops": (25, 65),
    "bottoms": (35, 85),
    "outerwear": (75, 150),
    "shoes": (45, 130),
    "accessories": (15, 60),
    "swimwear": (30, 80),
    "activewear": (28, 70),
}

EVENT_TYPES = ["page_view", "email_open", "email_click", "ad_click", "organic_visit", "referral_click"]
CHANNELS = ["paid_social", "organic_social", "email", "paid_search", "organic_search", "direct", "referral", "affiliate"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


def generate_products():
    """Generate 200 product SKUs across 8 categories."""
    print("Generating products...")
    products = []
    product_id = 1

    # Distribute products across categories (roughly equal, with some variation)
    products_per_cat = N_PRODUCTS // len(CATEGORIES)
    remainder = N_PRODUCTS % len(CATEGORIES)

    for i, category in enumerate(CATEGORIES):
        n = products_per_cat + (1 if i < remainder else 0)
        subcats = SUBCATEGORIES[category]
        price_low, price_high = PRICE_RANGES[category]

        for j in range(n):
            subcat = subcats[j % len(subcats)]
            base_price = round(np.random.uniform(price_low, price_high), 2)
            sku = f"CT-{category[:3].upper()}-{subcat[:3].upper()}-{product_id:04d}"

            products.append({
                "product_id": product_id,
                "sku": sku,
                "product_name": f"{fake.color_name()} {subcat.replace('_', ' ').title()}",
                "category": category,
                "subcategory": subcat,
                "base_price": base_price,
                "cost_price": round(base_price * np.random.uniform(0.3, 0.5), 2),
                "is_active": np.random.choice([True, False], p=[0.92, 0.08]),
                "created_at": fake.date_time_between(
                    start_date=START_DATE - timedelta(days=180),
                    end_date=START_DATE + timedelta(days=90),
                ),
            })
            product_id += 1

    return pd.DataFrame(products)


def generate_customers():
    """Generate 30K customers with heavier recent signups."""
    print("Generating customers...")
    # Heavier recent signups: exponential distribution biased toward recent
    days_range = (END_DATE - START_DATE).days
    # Use beta distribution to skew toward recent dates
    random_days = np.random.beta(2, 5, N_CUSTOMERS) * days_range
    signup_dates = [START_DATE + timedelta(days=int(d)) for d in random_days]

    customers = []
    for i in range(N_CUSTOMERS):
        customer_id = i + 1
        signup_dt = signup_dates[i]

        customers.append({
            "customer_id": customer_id,
            "email": f"customer_{customer_id}@{fake.free_email_domain()}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "gender": np.random.choice(["F", "M", "NB"], p=[0.62, 0.33, 0.05]),
            "age": int(np.clip(np.random.normal(32, 10), 18, 70)),
            "state": np.random.choice(US_STATES),
            "signup_at": signup_dt,
            "email_opt_in": np.random.choice([True, False], p=[0.72, 0.28]),
            "is_deleted": np.random.choice([True, False], p=[0.02, 0.98]),
        })

    return pd.DataFrame(customers)


def generate_orders(customers_df):
    """
    Generate 55K orders with power-law frequency distribution.
    Most customers have 1 order; a few have many.
    """
    print("Generating orders...")
    active_customers = customers_df[~customers_df["is_deleted"]].copy()
    customer_ids = active_customers["customer_id"].values
    signup_dates = active_customers.set_index("customer_id")["signup_at"].to_dict()

    # Power-law: assign order counts per customer
    # ~68% of purchasing customers get 1 order, then decreasing
    n_active = len(customer_ids)

    # 32% of customers never purchase
    purchasing_mask = np.random.random(n_active) >= 0.32
    n_purchasing = purchasing_mask.sum()

    order_counts = np.zeros(n_active, dtype=int)
    raw_counts = np.random.zipf(1.8, n_purchasing)
    raw_counts = np.clip(raw_counts, 1, 30)

    # Scale purchasing customers to hit target
    total = raw_counts.sum()
    scale_factor = N_ORDERS / total
    raw_counts = np.round(raw_counts * scale_factor).astype(int)
    raw_counts = np.maximum(raw_counts, 1)
    order_counts[purchasing_mask] = raw_counts

    # Fine-tune to hit exact target
    current_total = order_counts.sum()
    diff = N_ORDERS - current_total
    purchasing_idx = np.where(purchasing_mask)[0]
    if diff > 0:
        boost_idx = np.random.choice(purchasing_idx, size=abs(diff), replace=True)
        for idx in boost_idx:
            order_counts[idx] += 1
    elif diff < 0:
        multi_idx = np.where(order_counts > 1)[0]
        reduce_idx = np.random.choice(multi_idx, size=min(abs(diff), len(multi_idx)), replace=True)
        for idx in reduce_idx:
            order_counts[idx] -= 1

    orders = []
    order_id = 1

    # Holiday spike dates
    holiday_dates = [
        (datetime(2024, 11, 25), datetime(2024, 12, 2)),   # Black Friday 2024
        (datetime(2024, 12, 15), datetime(2024, 12, 25)),  # Christmas 2024
        (datetime(2025, 2, 10), datetime(2025, 2, 15)),    # Valentine's 2025
        (datetime(2025, 5, 5), datetime(2025, 5, 12)),     # Mother's Day 2025
        (datetime(2025, 6, 15), datetime(2025, 7, 5)),     # Summer sale 2025
        (datetime(2025, 11, 24), datetime(2025, 12, 1)),   # Black Friday 2025
        (datetime(2025, 12, 15), datetime(2025, 12, 25)),  # Christmas 2025
    ]

    for i, cid in enumerate(customer_ids):
        n_orders = order_counts[i]
        if n_orders == 0:
            continue

        signup_dt = signup_dates[cid]
        # Orders must be after signup
        earliest_order = signup_dt + timedelta(hours=np.random.randint(1, 72))
        latest_order = END_DATE

        if earliest_order >= latest_order:
            continue

        # Pre-generate evenly spaced order dates for this customer
        available_days = (latest_order - earliest_order).days
        if n_orders > 1 and available_days > 0:
            # Space orders evenly across available range with jitter
            spacing = available_days / n_orders
            order_timestamps = []
            for j in range(n_orders):
                base_day = int(j * spacing)
                jitter = np.random.randint(0, max(1, int(spacing * 0.8)))
                day_offset = min(base_day + jitter, available_days - 1)
                order_timestamps.append(earliest_order + timedelta(days=day_offset, hours=np.random.randint(8, 22)))
            order_timestamps.sort()
        else:
            order_timestamps = [earliest_order + timedelta(days=np.random.randint(0, max(1, available_days)))]

        for j, order_dt in enumerate(order_timestamps):

            status = np.random.choice(
                ["completed", "completed", "completed", "completed",
                 "completed", "shipped", "processing", "cancelled", "refunded"],
                p=[0.55, 0.15, 0.05, 0.05, 0.05, 0.05, 0.03, 0.04, 0.03],
            )

            discount_pct = 0.0
            if np.random.random() < 0.25:
                discount_pct = np.random.choice([0.10, 0.15, 0.20, 0.25, 0.30])

            orders.append({
                "order_id": order_id,
                "customer_id": cid,
                "order_at": order_dt,
                "status": status,
                "discount_pct": discount_pct,
                "shipping_cost": round(np.random.choice([0, 5.99, 7.99, 9.99], p=[0.3, 0.3, 0.25, 0.15]), 2),
                "is_deleted": False,
            })
            order_id += 1

    df = pd.DataFrame(orders)
    # Trim or pad to target
    if len(df) > N_ORDERS:
        df = df.sample(n=N_ORDERS, random_state=42).reset_index(drop=True)
        df["order_id"] = range(1, N_ORDERS + 1)
    print(f"  Generated {len(df)} orders")
    return df


def generate_order_items(orders_df, products_df):
    """Generate ~95K order items across orders."""
    print("Generating order items...")
    product_ids = products_df["product_id"].values
    product_prices = products_df.set_index("product_id")["base_price"].to_dict()

    order_ids = orders_df["order_id"].values
    order_discounts = orders_df.set_index("order_id")["discount_pct"].to_dict()

    # Distribute items per order: most orders 1-3 items, some up to 6
    n_orders = len(order_ids)
    items_per_order = np.random.choice([1, 1, 2, 2, 2, 3, 3, 4, 5, 6], size=n_orders)

    # Scale to hit target
    total_items = items_per_order.sum()
    scale = N_ORDER_ITEMS / total_items
    items_per_order = np.round(items_per_order * scale).astype(int)
    items_per_order = np.maximum(items_per_order, 1)

    order_items = []
    item_id = 1

    for i, oid in enumerate(order_ids):
        n_items = items_per_order[i]
        # Pick products (no duplicates within order)
        selected = np.random.choice(product_ids, size=min(n_items, len(product_ids)), replace=False)
        discount = order_discounts.get(oid, 0.0)

        for pid in selected:
            qty = np.random.choice([1, 1, 1, 2, 2, 3], p=[0.50, 0.15, 0.10, 0.10, 0.10, 0.05])
            base_price = product_prices[pid]
            unit_price = round(base_price * (1 - discount), 2)

            order_items.append({
                "order_item_id": item_id,
                "order_id": oid,
                "product_id": pid,
                "quantity": qty,
                "unit_price": unit_price,
                "total_price": round(unit_price * qty, 2),
            })
            item_id += 1

    df = pd.DataFrame(order_items)
    if len(df) > N_ORDER_ITEMS:
        df = df.head(N_ORDER_ITEMS)
        df["order_item_id"] = range(1, N_ORDER_ITEMS + 1)
    print(f"  Generated {len(df)} order items")
    return df


def generate_payments(orders_df, order_items_df):
    """Generate Stripe payments matching order totals."""
    print("Generating payments...")
    # Calculate order totals from items
    order_totals = order_items_df.groupby("order_id")["total_price"].sum().reset_index()
    order_totals.columns = ["order_id", "subtotal"]

    orders_with_totals = orders_df.merge(order_totals, on="order_id", how="left")
    orders_with_totals["subtotal"] = orders_with_totals["subtotal"].fillna(0)

    payments = []
    for _, row in orders_with_totals.iterrows():
        subtotal = row["subtotal"]
        shipping = row["shipping_cost"]
        total = round(subtotal + shipping, 2)

        status_map = {
            "completed": "succeeded",
            "shipped": "succeeded",
            "processing": "succeeded",
            "cancelled": "cancelled",
            "refunded": "refunded",
        }

        payments.append({
            "payment_id": f"pi_{hashlib.md5(str(row['order_id']).encode()).hexdigest()[:24]}",
            "order_id": row["order_id"],
            "amount": total,
            "currency": "usd",
            "payment_method": np.random.choice(
                PAYMENT_METHODS,
                p=[0.40, 0.20, 0.18, 0.12, 0.10],
            ),
            "status": status_map.get(row["status"], "succeeded"),
            "created_at": row["order_at"] + timedelta(seconds=np.random.randint(1, 120)),
        })

    return pd.DataFrame(payments)


def generate_events(customers_df, orders_df):
    """
    Generate 400K customer events with proper timestamp sequencing.
    Events should happen before orders, not after.
    """
    print("Generating events...")
    active_customers = customers_df[~customers_df["is_deleted"]].copy()
    customer_ids = active_customers["customer_id"].values
    signup_dates = active_customers.set_index("customer_id")["signup_at"].to_dict()

    # Get first order date per customer for sequencing
    first_orders = orders_df.groupby("customer_id")["order_at"].min().to_dict()

    events = []
    event_id = 1

    # Distribute events: more events for customers with orders
    has_order = set(orders_df["customer_id"].unique())
    events_per_customer = []
    for cid in customer_ids:
        if cid in has_order:
            events_per_customer.append(max(1, int(np.random.exponential(18))))
        else:
            events_per_customer.append(max(1, int(np.random.exponential(5))))

    total_events = sum(events_per_customer)
    scale = N_EVENTS / total_events

    for i, cid in enumerate(customer_ids):
        n_events = max(1, int(events_per_customer[i] * scale))
        signup_dt = signup_dates[cid]

        for _ in range(n_events):
            # Events can happen from signup until end date
            event_dt = fake.date_time_between(
                start_date=signup_dt,
                end_date=END_DATE,
            )

            # Determine event type and channel
            # Attribution-eligible: ad_click, email_click, organic_visit, referral_click
            event_type = np.random.choice(
                EVENT_TYPES,
                p=[0.22, 0.10, 0.15, 0.22, 0.18, 0.13],
            )

            # Map event types to likely channels
            if event_type in ("ad_click",):
                channel = np.random.choice(["paid_social", "paid_search"], p=[0.6, 0.4])
            elif event_type in ("email_open", "email_click"):
                channel = "email"
            elif event_type == "organic_visit":
                channel = np.random.choice(["organic_search", "direct", "organic_social"], p=[0.5, 0.3, 0.2])
            elif event_type == "referral_click":
                channel = np.random.choice(["referral", "affiliate"], p=[0.6, 0.4])
            else:
                channel = np.random.choice(CHANNELS, p=[0.20, 0.10, 0.15, 0.15, 0.15, 0.10, 0.08, 0.07])

            # Page URL for page views
            page_url = None
            if event_type == "page_view":
                page_url = np.random.choice([
                    "/", "/collections/dresses", "/collections/tops",
                    "/collections/new-arrivals", "/collections/sale",
                    "/products/detail", "/cart", "/checkout",
                    "/account", "/about",
                ])

            events.append({
                "event_id": event_id,
                "customer_id": cid,
                "event_type": event_type,
                "channel": channel,
                "event_at": event_dt,
                "page_url": page_url,
                "session_id": f"sess_{hashlib.md5(f'{cid}_{event_dt}'.encode()).hexdigest()[:16]}",
            })
            event_id += 1

            if event_id > N_EVENTS:
                break
        if event_id > N_EVENTS:
            break

    df = pd.DataFrame(events)
    print(f"  Generated {len(df)} events")
    return df


def load_to_postgres(engine, dataframes):
    """Load all dataframes into PostgreSQL."""
    print("\nLoading data into PostgreSQL...")

    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        conn.commit()

    table_map = {
        "customers": "raw_customers",
        "products": "raw_products",
        "orders": "raw_orders",
        "order_items": "raw_order_items",
        "payments": "raw_payments",
        "events": "raw_events",
    }

    # Drop existing tables with CASCADE to handle view dependencies
    with engine.connect() as conn:
        for table_name in table_map.values():
            conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{table_name} CASCADE"))
        conn.commit()

    for name, table_name in table_map.items():
        df = dataframes[name]
        print(f"  Loading {table_name}: {len(df)} rows...")
        df.to_sql(
            table_name,
            engine,
            schema=SCHEMA,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=5000,
        )

    print("Data loading complete!")


def main():
    print("=" * 60)
    print("Coastal Threads Analytics — Synthetic Data Generator")
    print("=" * 60)

    engine = create_engine(DB_URL)

    # Generate data
    products_df = generate_products()
    customers_df = generate_customers()
    orders_df = generate_orders(customers_df)
    order_items_df = generate_order_items(orders_df, products_df)
    payments_df = generate_payments(orders_df, order_items_df)
    events_df = generate_events(customers_df, orders_df)

    # Summary
    print("\n--- Data Summary ---")
    print(f"  Products:    {len(products_df):>8,}")
    print(f"  Customers:   {len(customers_df):>8,}")
    print(f"  Orders:      {len(orders_df):>8,}")
    print(f"  Order Items: {len(order_items_df):>8,}")
    print(f"  Payments:    {len(payments_df):>8,}")
    print(f"  Events:      {len(events_df):>8,}")

    # Load to PostgreSQL
    dataframes = {
        "customers": customers_df,
        "products": products_df,
        "orders": orders_df,
        "order_items": order_items_df,
        "payments": payments_df,
        "events": events_df,
    }

    load_to_postgres(engine, dataframes)

    # Verify
    print("\n--- Verification ---")
    with engine.connect() as conn:
        for table in ["raw_customers", "raw_products", "raw_orders",
                      "raw_order_items", "raw_payments", "raw_events"]:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.{table}"))
            count = result.scalar()
            print(f"  {table}: {count:,} rows")

    print("\nDone!")


if __name__ == "__main__":
    main()
