#!/usr/bin/env python3
"""
Postgres Database Initialization Script
Populates CUSTOMER_DATA table with 10,000 realistic records for BI Agent testing.

Usage:
    python scripts/init_postgres_data.py

Prerequisites:
    - Postgres container must be running and healthy
    - pip install psycopg[binary] faker
"""

import random
import sys
import os
import time
from datetime import datetime, timedelta
from typing import List, Tuple

try:
    import psycopg
    from psycopg import sql
except ImportError:
    print("ERROR: psycopg not installed. Run: pip install \"psycopg[binary]\"")
    sys.exit(1)

try:
    from faker import Faker
except ImportError:
    print("ERROR: faker not installed. Run: pip install faker")
    sys.exit(1)

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "postgres")

# Data generation constants
PRODUCTS = [
    ("ICT_SOLUTIONS", ["Cybersecurity Audit", "Managed IT Services", "Cloud Migration", "Data Analytics Platform", "Network Infrastructure"]),
    ("FINANCIAL_SERVICES", ["Payment Gateway", "Invoice Financing", "Trade Finance", "Cash Management", "FX Solutions"]),
    ("TELECOM", ["Enterprise Voice", "Data Connectivity", "IoT Platform", "Unified Communications", "Mobile Solutions"]),
    ("CLOUD_SERVICES", ["IaaS", "PaaS", "SaaS Integration", "Disaster Recovery", "Hybrid Cloud"]),
    ("CONSULTING", ["Digital Transformation", "Process Optimization", "Strategy Advisory", "Change Management", "Training Services"]),
]

SEGMENTS = ["STARTUP", "SME", "LARGE ENTERPRISE", "CORPORATE", "GOVERNMENT"]
TRIBES = ["ENTERPRISE SOLUTIONS", "DIGITAL BANKING", "RETAIL SERVICES", "WHOLESALE MARKETS", "PUBLIC SECTOR"]
SQUADS = [
    "ICT,MEDIA & ENTERTAINMENT",
    "FINANCIAL SERVICES",
    "HEALTHCARE & PHARMA",
    "RETAIL & DISTRIBUTION",
    "ENERGY & UTILITIES",
    "TRANSPORTATION & LOGISTICS",
    "EDUCATION & RESEARCH",
]
SECTORS = [
    "MANUFACTURING",
    "ICT & TECHNOLOGY",
    "BANKING & FINANCE",
    "HEALTHCARE",
    "RETAIL",
    "ENERGY",
    "GOVERNMENT",
    "EDUCATION",
    "AGRICULTURE",
    "REAL ESTATE",
]

def generate_customer_id(segment: str) -> str:
    """Generate a customer ID based on segment."""
    prefix_map = {
        "STARTUP": "1-STR-",
        "SME": "1-SME-",
        "LARGE ENTERPRISE": "1-ENT-",
        "CORPORATE": "1-CORP-",
        "GOVERNMENT": "1-GOV-",
    }
    prefix = prefix_map.get(segment, "1-CORP-")
    return f"{prefix}{random.randint(100000000, 999999999)}"

def generate_month_string(date: datetime) -> str:
    """Generate month string like '25-Jul' from date."""
    year_short = str(date.year)[-2:]
    month_abbr = date.strftime("%b")
    return f"{year_short}-{month_abbr}"

def generate_records(num_records: int, faker: Faker) -> List[Tuple]:
    """Generate realistic CUSTOMER_DATA records."""
    records = []
    
    # Generate a pool of customers (about 500 unique customers)
    num_customers = min(500, num_records // 20)
    customers = []
    for _ in range(num_customers):
        segment = random.choice(SEGMENTS)
        customers.append({
            "customer_id": generate_customer_id(segment),
            "customer_name": faker.company(),
            "segment": segment,
            "tribe": random.choice(TRIBES),
            "squad": random.choice(SQUADS),
            "sector": random.choice(SECTORS),
        })
    
    # Generate date range (last 12 months)
    end_date = datetime(2025, 7, 31)
    start_date = datetime(2025, 1, 1)
    date_range = (end_date - start_date).days
    
    for i in range(num_records):
        random_days = random.randint(0, date_range)
        record_date = start_date + timedelta(days=random_days)
        date_str = record_date.strftime("%d/%m/%Y")
        month_str = generate_month_string(record_date)
        
        product, sub_products = random.choice(PRODUCTS)
        sub_product = random.choice(sub_products)
        
        customer = random.choice(customers)
        
        revenue = round(random.uniform(1000, 150000), 2)
        used_resources = random.randint(1000, 100000)
        balance_resources = random.randint(1000, 80000)
        value_balances = round(random.uniform(500, 50000), 2)
        
        record = (
            date_str,
            month_str,
            product,
            sub_product,
            customer["customer_id"],
            customer["customer_name"],
            customer["segment"],
            customer["tribe"],
            customer["squad"],
            customer["sector"],
            str(revenue),
            str(used_resources),
            str(balance_resources),
            str(value_balances),
        )
        records.append(record)
        
        if (i + 1) % 1000 == 0:
            print(f"  Generated {i + 1:,} records...")
    
    return records

def wait_for_postgres(max_retries: int = 30, retry_interval: int = 2) -> bool:
    """Wait for Postgres to be ready."""
    print(f"Waiting for Postgres at {POSTGRES_HOST}:{POSTGRES_PORT}...")
    
    for attempt in range(max_retries):
        try:
            conn = psycopg.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname="postgres", # Connect to default db first
                connect_timeout=5,
            )
            conn.close()
            print("[OK] Postgres is ready!")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  Attempt {attempt + 1}/{max_retries}: Waiting... ({str(e)[:50]})")
                time.sleep(retry_interval)
            else:
                print(f"[FAIL] Postgres not ready after {max_retries} attempts")
                return False
    return False

def init_database():
    """Initialize the database and create table."""
    print("\n" + "=" * 60)
    print("POSTGRES DATABASE INITIALIZATION")
    print("=" * 60)
    
    if not wait_for_postgres():
        print("\nERROR: Postgres is not available.")
        sys.exit(1)
    
    # Connect
    conn_info = f"host={POSTGRES_HOST} port={POSTGRES_PORT} user={POSTGRES_USER} password={POSTGRES_PASSWORD} dbname={POSTGRES_DATABASE}"
    try:
        conn = psycopg.connect(conn_info, autocommit=True)
    except psycopg.OperationalError:
        # If DB doesn't exist, connect to postgres and create it
        print(f"Database '{POSTGRES_DATABASE}' not found, creating...")
        tmp_conn = psycopg.connect(f"host={POSTGRES_HOST} port={POSTGRES_PORT} user={POSTGRES_USER} password={POSTGRES_PASSWORD} dbname=postgres", autocommit=True)
        tmp_conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(POSTGRES_DATABASE)))
        tmp_conn.close()
        conn = psycopg.connect(conn_info, autocommit=True)

    cursor = conn.cursor()
    
    # Drop and Create Table
    print("Preparing CUSTOMER_DATA table...")
    cursor.execute("DROP TABLE IF EXISTS CUSTOMER_DATA")
    
    create_table_sql = """
    CREATE TABLE CUSTOMER_DATA (
        DATE VARCHAR(20),
        MONTH VARCHAR(10),
        PRODUCT VARCHAR(50),
        SUB_PRODUCT VARCHAR(100),
        CUSTOMER_ID VARCHAR(30),
        CUSTOMER VARCHAR(200),
        SEGMENT VARCHAR(30),
        TRIBE VARCHAR(50),
        SQUAD VARCHAR(50),
        SECTOR VARCHAR(50),
        REVENUE NUMERIC(20, 2),
        USED_RESOURCES INTEGER,
        BALANCE_RESOURCES INTEGER,
        VALUE_BALANCES NUMERIC(20, 2)
    )
    """
    cursor.execute(create_table_sql)
    print("[OK] CUSTOMER_DATA table created")
    
    # Generate data
    print("\nGenerating 10,000 records...")
    faker = Faker()
    Faker.seed(42)
    random.seed(42)
    records = generate_records(10000, faker)
    
    # Insert data
    print("\nInserting records...")
    insert_sql = """
    INSERT INTO CUSTOMER_DATA 
    (DATE, MONTH, PRODUCT, SUB_PRODUCT, CUSTOMER_ID, CUSTOMER, SEGMENT, TRIBE, SQUAD, SECTOR, REVENUE, USED_RESOURCES, BALANCE_RESOURCES, VALUE_BALANCES)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
        print(f"  Inserted {i + len(batch):,} / {len(records):,} records...")
    
    # Verification
    cursor.execute("SELECT COUNT(*) FROM CUSTOMER_DATA")
    count = cursor.fetchone()[0]
    print(f"\n[OK] CUSTOMER_DATA contains {count:,} records")
    
    conn.close()
    print("\n" + "=" * 60)
    print("[OK] DATABASE INITIALIZATION COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    init_database()
