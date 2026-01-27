#!/usr/bin/env python3
"""
Oracle Database Initialization Script
Populates CUSTOMER_DATA table with 10,000 realistic records for BI Agent testing.

Usage:
    python scripts/init_oracle_data.py

Prerequisites:
    - Oracle 23ai Free container must be running and healthy
    - pip install oracledb faker
"""

import random
import sys
import os
import time
from datetime import datetime, timedelta
from typing import List, Tuple
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

try:
    import oracledb
except ImportError:
    print("ERROR: oracledb not installed. Run: pip install oracledb")
    sys.exit(1)

try:
    from faker import Faker
except ImportError:
    print("ERROR: faker not installed. Run: pip install faker")
    sys.exit(1)

# Configuration
ORACLE_HOST = os.getenv("ORACLE_HOST", "127.0.0.1")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", 1521))
ORACLE_USER = os.getenv("ORACLE_USERNAME", "system")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "password")
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")

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
    
    # Generate date range (full year 2025)
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)
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
            revenue,
            used_resources,
            balance_resources,
            value_balances,
        )
        records.append(record)
        
        if (i + 1) % 1000 == 0:
            print(f"  Generated {i + 1:,} records...")
    
    return records

def wait_for_oracle(max_retries: int = 180, retry_interval: int = 5) -> bool:
    """
    Wait for Oracle to be ready with extended timeout (up to 15 minutes).
    Oracle 23ai Free can take several minutes to initialize on first boot.
    """
    print(f"Waiting for Oracle at {ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}...")
    print("NOTE: Oracle 23ai can take 5-10 minutes to initialize the PDB on first run. Please be patient.")
    
    for attempt in range(max_retries):
        try:
            conn = oracledb.connect(
                user=ORACLE_USER,
                password=ORACLE_PASSWORD,
                dsn=f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}"
            )
            conn.close()
            print("\n[OK] Oracle is ready!")
            return True
        except Exception as e:
            error_msg = str(e).strip()
            # If we see DPY-6001 or DPY-6005, it means host is up but service isn't ready
            if "DPY-6001" in error_msg or "DPY-6005" in error_msg:
                status_note = "Listener up, but service not yet registered"
            elif "DPY-4001" in error_msg:
                status_note = "Network unreachable (check host/port)"
            else:
                status_note = "Initializing..."

            if attempt < max_retries - 1:
                # Show full error every 10 attempts, otherwise just status
                if attempt % 10 == 0:
                    print(f"  Attempt {attempt + 1}/{max_retries}: {status_note} ({error_msg[:150]}...)")
                else:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                time.sleep(retry_interval)
            else:
                print(f"\n[FAIL] Oracle not ready after {max_retries} attempts. Last error: {error_msg}")
                return False
    return False

def init_database():
    """Initialize the database and create table."""
    print("\n" + "=" * 60)
    print("ORACLE DATABASE INITIALIZATION")
    print("=" * 60)
    
    if not wait_for_oracle():
        print("\nERROR: Oracle is not available.")
        sys.exit(1)
    
    # Connect
    try:
        conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}"
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"ERROR: Failed to connect to Oracle: {e}")
        sys.exit(1)

    # Drop and Create Table
    print("Preparing CUSTOMER_DATA table...")
    try:
        cursor.execute("DROP TABLE CUSTOMER_DATA")
    except oracledb.DatabaseError as e:
        error, = e.args
        if error.code != 942: # ORA-00942: table or view does not exist
            raise

    create_table_sql = """
    CREATE TABLE CUSTOMER_DATA (
        "DATE" VARCHAR2(20),
        "MONTH" VARCHAR2(10),
        PRODUCT VARCHAR2(50),
        SUB_PRODUCT VARCHAR2(100),
        CUSTOMER_ID VARCHAR2(30),
        CUSTOMER VARCHAR2(200),
        SEGMENT VARCHAR2(30),
        TRIBE VARCHAR2(50),
        SQUAD VARCHAR2(50),
        SECTOR VARCHAR2(50),
        REVENUE NUMBER(20, 2),
        USED_RESOURCES NUMBER(20),
        BALANCE_RESOURCES NUMBER(20),
        VALUE_BALANCES NUMBER(20, 2)
    )
    """
    cursor.execute(create_table_sql)
    print("[OK] CUSTOMER_DATA table created")
    
    # Generate data
    print("\nGenerating 100,000 records...")
    faker = Faker()
    Faker.seed(42)
    random.seed(42)
    records = generate_records(10000, faker)
    
    # Insert data
    print("\nInserting records...")
    insert_sql = """
    INSERT INTO CUSTOMER_DATA 
    ("DATE", "MONTH", PRODUCT, SUB_PRODUCT, CUSTOMER_ID, CUSTOMER, SEGMENT, TRIBE, SQUAD, SECTOR, REVENUE, USED_RESOURCES, BALANCE_RESOURCES, VALUE_BALANCES)
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)
    """
    
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        print(f"  Inserted {i + len(batch):,} / {len(records):,} records...")
    
    # Verification
    cursor.execute("SELECT COUNT(*) FROM CUSTOMER_DATA")
    count = cursor.fetchone()[0]
    print(f"\n[OK] CUSTOMER_DATA contains {count:,} records")
    
    conn.close()
    print("\n" + "=" * 60)
    print("[OK] ORACLE INITIALIZATION COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    init_database()
