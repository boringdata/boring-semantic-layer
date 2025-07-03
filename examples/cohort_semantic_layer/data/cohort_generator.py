#!/usr/bin/env python3
"""
E-commerce Data Generator for Cohort Analysis

This CLI tool generates realistic orders.csv and customers.csv files
with seasonal patterns, customer churn, and geographic distribution.
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from typing import Dict, List, Set
import uuid

# Optional pandas import for parquet support
try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class DataGenerator:
    def __init__(self, start_date: str, end_date: str):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # Country distribution (France gets slightly higher weight)
        self.countries = {
            "France": 0.25,
            "Germany": 0.15,
            "United Kingdom": 0.12,
            "USA": 0.15,
            "Italy": 0.10,
            "Spain": 0.08,
            "Netherlands": 0.06,
            "Belgium": 0.05,
            "Denmark": 0.02,
            "Switzerland": 0.02,
        }

        # Customer lifecycle parameters
        self.customer_lifespan_months = (
            1,
            18,
        )  # Min/max months a customer stays active
        self.monthly_churn_rate = 0.15  # 15% of customers churn each month
        self.new_customer_rate_range = (
            0.15,
            0.45,
        )  # 15-45% new customers each month (random)

        # Order patterns
        self.orders_per_customer_per_month = (
            0.25,
            2.0,
        )  # Min/max orders per active customer (50% reduction)
        self.base_order_amount = (20, 500)  # Base order amount range
        self.christmas_multiplier = 2.5  # Christmas order amount multiplier
        self.christmas_months = [11, 12]  # November and December

        # Initialize data structures
        self.customers: Dict[str, Dict] = {}
        self.orders: List[Dict] = []
        self.active_customers: Set[str] = set()

    def is_christmas_period(self, date: datetime) -> bool:
        """Check if date is in Christmas period (Nov-Dec)"""
        return date.month in self.christmas_months

    def generate_customer_id(self) -> str:
        """Generate unique customer ID"""
        return f"cust_{uuid.uuid4().hex[:8]}"

    def generate_order_id(self) -> str:
        """Generate unique order ID"""
        return f"ord_{uuid.uuid4().hex[:8]}"

    def select_country(self) -> str:
        """Select country based on weighted distribution"""
        countries = list(self.countries.keys())
        weights = list(self.countries.values())
        return random.choices(countries, weights=weights)[0]

    def calculate_order_amount(self, date: datetime) -> int:
        """Calculate order amount with seasonal adjustments"""
        base_amount = random.randint(*self.base_order_amount)

        if self.is_christmas_period(date):
            base_amount = int(base_amount * self.christmas_multiplier)

        return base_amount

    def should_customer_churn(self, customer: Dict, current_date: datetime) -> bool:
        """Determine if customer should churn based on their lifecycle"""
        days_since_first_order = (current_date - customer["first_order_date"]).days
        months_active = days_since_first_order / 30.44  # Average days per month

        # Higher churn probability as customer ages
        base_churn_rate = self.monthly_churn_rate
        age_multiplier = 1 + (months_active / 12)  # Increases over time
        churn_probability = base_churn_rate * age_multiplier

        return random.random() < churn_probability

    def generate_customers_for_month(self, year: int, month: int) -> List[str]:
        """Generate new customers for a specific month"""
        new_customers = []

        # Calculate how many new customers to add (random each month)
        countries_count = len(self.countries)
        customers_per_country = 100
        total_possible_customers = countries_count * customers_per_country

        # Random new customer rate for this month
        random_new_customer_rate = random.uniform(*self.new_customer_rate_range)
        new_customer_count = int(total_possible_customers * random_new_customer_rate)

        # Add some randomness to the count itself (±20%)
        variation = int(new_customer_count * 0.2)
        new_customer_count = random.randint(
            max(1, new_customer_count - variation), new_customer_count + variation
        )

        for _ in range(new_customer_count):
            customer_id = self.generate_customer_id()
            country = self.select_country()

            # Set customer lifecycle
            lifespan_months = random.randint(*self.customer_lifespan_months)
            first_order_date = datetime(year, month, random.randint(1, 28))

            self.customers[customer_id] = {
                "customer_id": customer_id,
                "country_name": country,
                "first_order_date": first_order_date,
                "lifespan_months": lifespan_months,
                "last_order_date": first_order_date,
            }

            self.active_customers.add(customer_id)
            new_customers.append(customer_id)

        return new_customers

    def process_customer_churn(self, current_date: datetime):
        """Process customer churn for current month"""
        customers_to_remove = []

        for customer_id in self.active_customers:
            customer = self.customers[customer_id]

            if self.should_customer_churn(customer, current_date):
                customers_to_remove.append(customer_id)

        for customer_id in customers_to_remove:
            self.active_customers.remove(customer_id)

    def generate_orders_for_month(self, year: int, month: int):
        """Generate orders for all active customers in a month"""
        current_date = datetime(year, month, 1)
        days_in_month = (
            (datetime(year, month + 1, 1) - timedelta(days=1)).day if month < 12 else 31
        )

        # Ensure minimum orders per month (5 countries × 100 customers × 250 orders = 125,000)
        countries_count = len(self.countries)
        min_orders = countries_count * 100 * 250  # 50% reduction from original 500

        orders_generated = 0

        for customer_id in self.active_customers:
            customer = self.customers[customer_id]

            # Determine number of orders for this customer this month
            orders_count = random.uniform(*self.orders_per_customer_per_month)

            # Christmas boost
            if self.is_christmas_period(current_date):
                orders_count *= 1.5

            orders_count = int(orders_count)

            for _ in range(orders_count):
                order_date = current_date + timedelta(
                    days=random.randint(0, days_in_month - 1)
                )

                # Skip if order date is beyond end date
                if order_date > self.end_date:
                    continue

                order = {
                    "order_id": self.generate_order_id(),
                    "order_date": order_date.strftime("%Y-%m-%d"),
                    "order_amount": self.calculate_order_amount(order_date),
                    "customer_id": customer_id,
                    "product_count": random.randint(1, 8),
                }

                self.orders.append(order)
                orders_generated += 1

                # Update customer's last order date
                customer["last_order_date"] = order_date

        # If we haven't met minimum orders, generate additional orders from random active customers
        while orders_generated < min_orders and self.active_customers:
            customer_id = random.choice(list(self.active_customers))
            order_date = current_date + timedelta(
                days=random.randint(0, days_in_month - 1)
            )

            if order_date > self.end_date:
                break

            order = {
                "order_id": self.generate_order_id(),
                "order_date": order_date.strftime("%Y-%m-%d"),
                "order_amount": self.calculate_order_amount(order_date),
                "customer_id": customer_id,
                "product_count": random.randint(1, 8),
            }

            self.orders.append(order)
            orders_generated += 1

    def generate_data(self):
        """Generate all data for the specified date range"""
        print(
            f"Generating data from {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}"
        )

        current_date = self.start_date.replace(day=1)  # Start from first day of month

        while current_date <= self.end_date:
            year = current_date.year
            month = current_date.month

            print(f"Processing {year}-{month:02d}...")

            # Generate new customers for this month
            new_customers = self.generate_customers_for_month(year, month)
            print(f"  Added {len(new_customers)} new customers")

            # Generate orders for all active customers
            self.generate_orders_for_month(year, month)

            # Process customer churn at end of month
            self.process_customer_churn(current_date)

            print(f"  Active customers: {len(self.active_customers)}")
            print(f"  Total orders so far: {len(self.orders)}")

            # Move to next month
            if month == 12:
                current_date = datetime(year + 1, 1, 1)
            else:
                current_date = datetime(year, month + 1, 1)

    def save_customers_csv(self, filename: str = "customers.csv"):
        """Save customers data to CSV file"""
        print(f"Saving customers data to {filename}...")

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["customer_id", "country_name"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for customer in self.customers.values():
                writer.writerow(
                    {
                        "customer_id": customer["customer_id"],
                        "country_name": customer["country_name"],
                    }
                )

        print(f"Saved {len(self.customers)} customers")

    def save_orders_csv(self, filename: str = "orders.csv"):
        """Save orders data to CSV file"""
        print(f"Saving orders data to {filename}...")

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "order_id",
                "order_date",
                "order_amount",
                "customer_id",
                "product_count",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for order in self.orders:
                writer.writerow(order)

        print(f"Saved {len(self.orders)} orders")

    def save_customers_parquet(self, filename: str = "customers.parquet"):
        """Save customers data to Parquet file"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is required for parquet support. Install with: pip install pandas pyarrow"
            )

        print(f"Saving customers data to {filename}...")

        # Prepare data for DataFrame
        customer_data = []
        for customer in self.customers.values():
            customer_data.append(
                {
                    "customer_id": customer["customer_id"],
                    "country_name": customer["country_name"],
                }
            )

        # Create DataFrame and save as parquet
        df = pd.DataFrame(customer_data)
        df.to_parquet(filename, engine="pyarrow", index=False)

        print(f"Saved {len(self.customers)} customers")

    def save_orders_parquet(self, filename: str = "orders.parquet"):
        """Save orders data to Parquet file"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is required for parquet support. Install with: pip install pandas pyarrow"
            )

        print(f"Saving orders data to {filename}...")

        # Create DataFrame and save as parquet
        df = pd.DataFrame(self.orders)
        df.to_parquet(filename, engine="pyarrow", index=False)

        print(f"Saved {len(self.orders)} orders")

    def print_statistics(self):
        """Print generation statistics"""
        print("\n=== Generation Statistics ===")
        print(f"Total customers: {len(self.customers)}")
        print(f"Total orders: {len(self.orders)}")

        # Country distribution
        country_counts = {}
        for customer in self.customers.values():
            country = customer["country_name"]
            country_counts[country] = country_counts.get(country, 0) + 1

        print("\nCustomer distribution by country:")
        for country, count in sorted(
            country_counts.items(), key=lambda x: x[1], reverse=True
        ):
            percentage = (count / len(self.customers)) * 100
            print(f"  {country}: {count} ({percentage:.1f}%)")

        # Order amount statistics
        order_amounts = [order["order_amount"] for order in self.orders]
        if order_amounts:
            print("\nOrder amounts:")
            print(f"  Min: ${min(order_amounts)}")
            print(f"  Max: ${max(order_amounts)}")
            print(f"  Average: ${sum(order_amounts) / len(order_amounts):.2f}")

        # Monthly order distribution
        monthly_orders = {}
        for order in self.orders:
            month = order["order_date"][:7]  # YYYY-MM format
            monthly_orders[month] = monthly_orders.get(month, 0) + 1

        print("\nOrders by month:")
        for month in sorted(monthly_orders.keys()):
            print(f"  {month}: {monthly_orders[month]:,} orders")


def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic e-commerce data for cohort analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cohort_generator.py --start-date 2023-01-01 --end-date 2023-12-31
  python cohort_generator.py -s 2024-01-01 -e 2024-06-30
  python cohort_generator.py --start-date 2023-01-01 --end-date 2024-12-31 --customers-file my_customers.csv
  python cohort_generator.py --start-date 2023-01-01 --end-date 2023-12-31 --format parquet
        """,
    )

    parser.add_argument(
        "--start-date",
        "-s",
        required=True,
        help="Start date for order generation (YYYY-MM-DD format)",
    )

    parser.add_argument(
        "--end-date",
        "-e",
        required=True,
        help="End date for order generation (YYYY-MM-DD format)",
    )

    parser.add_argument(
        "--orders-file",
        default="orders.csv",
        help="Output filename for orders CSV (default: orders.csv)",
    )

    parser.add_argument(
        "--customers-file",
        default="customers.csv",
        help="Output filename for customers CSV (default: customers.csv)",
    )

    parser.add_argument("--seed", type=int, help="Random seed for reproducible results")

    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output format for data files (default: csv)",
    )

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
        datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format")
        return 1

    # Set random seed if provided
    if args.seed:
        random.seed(args.seed)
        print(f"Using random seed: {args.seed}")

    # Check if parquet format requested without pandas
    if args.format == "parquet" and not PANDAS_AVAILABLE:
        print("Error: pandas and pyarrow are required for parquet output format")
        print("Install with: pip install pandas pyarrow")
        return 1

    # Generate data
    generator = DataGenerator(args.start_date, args.end_date)
    generator.generate_data()

    # Save files based on format
    if args.format == "parquet":
        # Update file extensions if they still have .csv
        customers_file = args.customers_file
        orders_file = args.orders_file

        if customers_file.endswith(".csv"):
            customers_file = customers_file[:-4] + ".parquet"
        if orders_file.endswith(".csv"):
            orders_file = orders_file[:-4] + ".parquet"

        generator.save_customers_parquet(customers_file)
        generator.save_orders_parquet(orders_file)
    else:
        customers_file = args.customers_file
        orders_file = args.orders_file
        generator.save_customers_csv(customers_file)
        generator.save_orders_csv(orders_file)

    # Print statistics
    generator.print_statistics()

    print("\n✅ Data generation complete!")
    print("Files created:")
    print(f"  - {customers_file}")
    print(f"  - {orders_file}")

    return 0


if __name__ == "__main__":
    exit(main())
