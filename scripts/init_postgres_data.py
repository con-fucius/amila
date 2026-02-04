#!/usr/bin/env python3
"""
Postgres Database Initialization Script
Creates and populates test tables for BI Agent testing.

Tables:
    - TEST_INFORMAT_CALL_DETLS (50,000 rows)
    - TEST_AGG_EBU_IFRS_DAY (50,000 rows)
    - TEST_ALLOT_DATA_HOUR (50,000 rows)

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
TOTAL_ROWS = 50000
BATCH_SIZE = 1000
DATE_START = datetime(2024, 1, 1)
DATE_END = datetime(2025, 12, 31)
DATE_RANGE_DAYS = (DATE_END - DATE_START).days

CALLER_PURPOSES = ["Inquiry", "Complaint", "Activation", "Billing", "Support", "Retention"]
GV_EXIT_CODES = ["0", "1", "2", "3", "4"]
TECH_RESULTS = ["SUCCESS", "FAILED", "TIMEOUT", "BUSY", "TRANSFERRED"]
RESULT_REASONS = ["User Hangup", "Network Error", "Agent Transfer", "Completed", "System Error"]
RESOURCE_ROLES = ["AGENT", "IVR", "QUEUE", "SUPERVISOR", "BOT"]
PLACE_NAMES = ["Main Office", "Branch", "Call Center", "Remote", "HQ"]
LANGUAGES = ["EN", "AR", "FR", "ES"]
SUBSCRIBER_TYPES = ["PREPAID", "POSTPAID", "BUSINESS"]
VB_THRESHOLDS = ["LOW", "MEDIUM", "HIGH"]
VB_VERIFIED_VALUES = ["Y", "N"]
SWITCH_NAMES = ["SWITCH_A", "SWITCH_B", "SWITCH_C", "SWITCH_D"]
MEDIA_NAMES = ["VOICE", "CHAT", "EMAIL", "SOCIAL"]
INTERACTION_TYPES = ["INBOUND", "OUTBOUND", "INTERNAL"]
SERVICE_SUBTYPES = ["GENERAL", "TECH", "BILLING", "RETENTION", "SALES"]
VIRTUAL_QUEUES = ["QUEUE_SUPPORT", "QUEUE_SALES", "QUEUE_BILLING", "QUEUE_VIP"]
DISPOSITION_CODES = ["COMPLETE", "TRANSFER", "ABANDON", "CALLBACK"]
CUSTOMER_SEGMENTS = ["CONSUMER", "SMB", "ENTERPRISE", "VIP"]
IVR_SS_ROUTES = ["ROUTE_A", "ROUTE_B", "ROUTE_C", "ROUTE_D"]
STOP_REASONS = ["NORMAL", "TIMEOUT", "AGENT_END", "CUSTOMER_END"]

REVN_TYPES = ["SERVICE", "USAGE", "SUBSCRIPTION", "ONE_TIME"]
REVN_SUB_TYPES = ["VOICE", "DATA", "SMS", "ROAMING", "VAS"]
PRODUCTS = ["MOBILE", "BROADBAND", "IOT", "CLOUD", "SECURITY"]
PAY_TYPES = ["PREPAID", "POSTPAID", "HYBRID"]
SEGMENTS = ["SMB", "ENTERPRISE", "PUBLIC", "CONSUMER"]
TRIBES = ["CORE", "DIGITAL", "ENTERPRISE", "PARTNER"]
SQUADS = ["SQUAD_A", "SQUAD_B", "SQUAD_C", "SQUAD_D"]
SECTORS = ["FINANCE", "RETAIL", "GOVERNMENT", "HEALTH", "EDU"]
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]

PRODUCT_LINES = ["MOBILE", "FIXED", "IOT", "CLOUD", "SECURITY"]
SERVICE_PLANS = ["BASIC", "STANDARD", "PREMIUM", "ENTERPRISE"]
APPLICATION_NAMES = ["STREAMING", "BROWSING", "VPN", "GAMING", "VOIP"]
APPLICATION_FAMILIES = ["MEDIA", "PRODUCTIVITY", "COMMUNICATION", "SECURITY"]


def truncate(value: str, max_len: int) -> str:
    if value is None:
        return ""
    return str(value)[:max_len]


def random_date() -> datetime:
    return DATE_START + timedelta(days=random.randint(0, DATE_RANGE_DAYS))


def random_datetime() -> datetime:
    base_date = random_date()
    seconds = random.randint(0, 86399)
    microseconds = random.randint(0, 999999)
    return base_date + timedelta(seconds=seconds, microseconds=microseconds)


def date_key(dt: datetime) -> int:
    return int(dt.strftime("%Y%m%d"))


def generate_call_details_row(faker: Faker) -> Tuple:
    start_time = random_datetime()
    routing = random.randint(0, 60)
    queue = random.randint(0, 120)
    ring = random.randint(0, 30)
    talk = random.randint(30, 900)
    hold = random.randint(0, 120)
    after_call = random.randint(0, 300)
    customer_talk = max(talk - hold, 0)
    total_duration = routing + queue + ring + talk + hold + after_call
    end_time = start_time + timedelta(seconds=total_duration)

    return (
        date_key(start_time),
        start_time,
        end_time,
        total_duration,
        random.randint(100000000, 999999999),
        random.randint(100000, 999999),
        truncate(faker.phone_number(), 50),
        truncate(faker.phone_number(), 50),
        random.randint(100000, 999999),
        random.choice(CALLER_PURPOSES),
        random.choice(GV_EXIT_CODES),
        random.choice(TECH_RESULTS),
        random.choice(RESULT_REASONS),
        random.choice(RESOURCE_ROLES),
        random.choice(PLACE_NAMES),
        truncate(faker.name(), 50),
        f"EMP{random.randint(10000, 99999)}",
        random.choice(LANGUAGES),
        random.choice(SUBSCRIBER_TYPES),
        f"SUB{random.randint(1000000, 9999999)}",
        random.choice(VB_THRESHOLDS),
        random.choice(VB_VERIFIED_VALUES),
        random.choice(SWITCH_NAMES),
        routing,
        queue,
        ring,
        talk,
        customer_talk,
        hold,
        after_call,
        random.choice(MEDIA_NAMES),
        random.choice(INTERACTION_TYPES),
        random.choice(SERVICE_SUBTYPES),
        truncate(random.choice(VIRTUAL_QUEUES), 150),
        random.choice(DISPOSITION_CODES),
        random.randint(1, 5),
        random.choice(CUSTOMER_SEGMENTS),
        random.choice(IVR_SS_ROUTES),
        random.choice(STOP_REASONS),
    )


def generate_ebu_ifrs_row(faker: Faker) -> Tuple:
    dt = random_date()
    gross = round(random.uniform(1000, 200000), 2)
    net = round(gross * random.uniform(0.6, 0.95), 2)
    return (
        date_key(dt),
        random.choice(REVN_TYPES),
        random.choice(REVN_SUB_TYPES),
        random.choice(PRODUCTS),
        random.randint(1000, 999999),
        f"SBSC{random.randint(100000, 999999)}",
        random.choice(PAY_TYPES),
        random.randint(100000, 999999),
        f"CHD{random.randint(1000, 9999)}",
        f"PRT{random.randint(1000, 9999)}",
        truncate(faker.company(), 300),
        f"EBU-{random.randint(1000, 9999)}",
        f"HOLD-{random.randint(1000, 9999)}",
        random.randint(0, 1),
        random.choice(SEGMENTS),
        random.choice(TRIBES),
        random.choice(SQUADS),
        random.choice(SECTORS),
        random.choice(REGIONS),
        gross,
        net,
    )


def generate_allot_data_row() -> Tuple:
    dt = random_date()
    hour = random.randint(0, 23)
    live_connections = random.randint(50, 5000)
    new_connections = random.randint(0, live_connections)
    dl_mb = round(random.uniform(10, 5000), 2)
    ul_mb = round(random.uniform(5, 2000), 2)
    activity_time = random.randint(60, 3600)
    rtx = random.randint(1000, 100000)
    total = rtx + random.randint(0, 200000)
    return (
        date_key(dt),
        hour,
        random.choice(PRODUCT_LINES),
        random.choice(SERVICE_PLANS),
        f"CUST-{random.randint(100000, 999999)}",
        random.choice(APPLICATION_NAMES),
        random.choice(APPLICATION_FAMILIES),
        live_connections,
        new_connections,
        dl_mb,
        ul_mb,
        activity_time,
        rtx,
        total,
    )


def populate_table(cursor, table_name: str, insert_sql: str, row_fn, total_rows: int, batch_size: int):
    print(f"\nPopulating {table_name} with {total_rows:,} rows...")
    inserted = 0
    while inserted < total_rows:
        current_batch = min(batch_size, total_rows - inserted)
        batch = [row_fn() for _ in range(current_batch)]
        cursor.executemany(insert_sql, batch)
        inserted += current_batch
        if inserted % (batch_size * 5) == 0 or inserted == total_rows:
            print(f"  Inserted {inserted:,} / {total_rows:,} rows into {table_name}...")

def wait_for_postgres(max_retries: int = 30, retry_interval: int = 2) -> bool:
    """Wait for Postgres to be ready with automatic localhost fallback."""
    import socket
    current_host = POSTGRES_HOST
    print(f"Waiting for Postgres at {current_host}:{POSTGRES_PORT}...")
    
    for attempt in range(max_retries):
        try:
            conn = psycopg.connect(
                host=current_host,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname="postgres",
                connect_timeout=5,
            )
            conn.close()
            print(f"\n[OK] Postgres is ready at {current_host}!")
            return True
        except Exception as e:
            error_msg = str(e)
            # Handle resolution errors for local execution
            if ("getaddrinfo failed" in error_msg or "could not connect to server" in error_msg) and current_host != "127.0.0.1":
                if attempt == 0:
                    print(f"  Note: Could not resolve '{current_host}'. Falling back to 127.0.0.1 for local execution.")
                current_host = "127.0.0.1"

            if attempt < max_retries - 1:
                if attempt % 5 == 0:
                    print(f"  Attempt {attempt + 1}/{max_retries}: Waiting for Postgres... ({error_msg[:100]})")
                else:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                time.sleep(retry_interval)
            else:
                print(f"\n[FAIL] Postgres not ready after {max_retries} attempts")
                return False
    return False

def init_database():
    """Initialize the database and create tables."""
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
        print(f"Database '{POSTGRES_DATABASE}' not found, creating...")
        tmp_conn = psycopg.connect(
            f"host={POSTGRES_HOST} port={POSTGRES_PORT} user={POSTGRES_USER} password={POSTGRES_PASSWORD} dbname=postgres",
            autocommit=True,
        )
        tmp_conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(POSTGRES_DATABASE)))
        tmp_conn.close()
        conn = psycopg.connect(conn_info, autocommit=True)

    cursor = conn.cursor()

    print("Preparing test tables...")
    for table in [
        "TEST_INFORMAT_CALL_DETLS",
        "TEST_AGG_EBU_IFRS_DAY",
        "TEST_ALLOT_DATA_HOUR",
    ]:
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}" ).format(sql.Identifier(table)))

    cursor.execute(
        sql.SQL("""
        CREATE TABLE {} (
            ID_DATE NUMERIC(22),
            START_TIME TIMESTAMP(6),
            END_TIME TIMESTAMP(6),
            TOTAL_CALL_DURATION NUMERIC(22),
            INTERACTION_ID NUMERIC(22),
            INTERACTION_RESOURCE_ID NUMERIC(22),
            SOURCE_ADDRESS VARCHAR(50),
            TARGET_ADDRESS VARCHAR(50),
            CONNID NUMERIC(22),
            CALLERPURPOSE VARCHAR(50),
            GVPEXITCODE VARCHAR(50),
            TECHNICAL_RESULT VARCHAR(50),
            RESULT_REASON VARCHAR(50),
            RESOURCE_ROLE VARCHAR(50),
            PLACE_NAME VARCHAR(50),
            RESOURCE_NAME VARCHAR(50),
            EMPLOYEE_ID VARCHAR(50),
            LANGUAGE VARCHAR(50),
            SUBSCRIBERTYPE VARCHAR(50),
            SUBSCRIBERVALUE VARCHAR(50),
            VB_THRESHOLD VARCHAR(50),
            VB_VERIFIED VARCHAR(50),
            SWITCH_NAME VARCHAR(50),
            ROUTING_POINT_DURATION NUMERIC(22),
            QUEUE_DURATION NUMERIC(22),
            RING_DURATION NUMERIC(22),
            TALK_DURATION NUMERIC(22),
            CUSTOMER_TALK_DURATION NUMERIC(22),
            HOLD_DURATION NUMERIC(22),
            AFTER_CALL_WORK_DURATION NUMERIC(22),
            MEDIA_NAME VARCHAR(50),
            INTERACTION_TYPE VARCHAR(50),
            SERVICE_SUBTYPE VARCHAR(50),
            VIRTUAL_QUEUE VARCHAR(150),
            DISPOSITION_CODE VARCHAR(50),
            HANDLE_COUNT NUMERIC(22),
            CUSTOMER_SEGMENT VARCHAR(200),
            IVR_SS_ROUTE VARCHAR(255),
            STOP_REASON VARCHAR(255)
        )
        """).format(sql.Identifier("TEST_INFORMAT_CALL_DETLS"))
    )
    print("[OK] TEST_INFORMAT_CALL_DETLS table created")

    cursor.execute(
        sql.SQL("""
        CREATE TABLE {} (
            ID_DATE NUMERIC(8, 0) NOT NULL,
            REVN_TYPE VARCHAR(30),
            REVN_SUB_TYPE VARCHAR(150),
            PRODUCT VARCHAR(250),
            ID_PDSV NUMERIC,
            NR_SBSC VARCHAR(40),
            SBSC_PAY_TYPE VARCHAR(50),
            ID_CST NUMERIC,
            CHILD_ACCT VARCHAR(30),
            PARENT_ACCT VARCHAR(50),
            PARENT_ACCT_NAME VARCHAR(300),
            EBU_CST_ID VARCHAR(800),
            EBU_HOLD_CST VARCHAR(800),
            ENTREPRISE_FLAG_OLD NUMERIC,
            EBU_CST_SGMN VARCHAR(800),
            EBU_CST_TRIBE VARCHAR(800),
            EBU_CST_SQUAD VARCHAR(800),
            EBU_CST_SECTOR VARCHAR(800),
            EBU_CST_REGION CHAR(20),
            GROSS_REVENUE NUMERIC,
            NET_REVENUE NUMERIC
        )
        """).format(sql.Identifier("TEST_AGG_EBU_IFRS_DAY"))
    )
    print("[OK] TEST_AGG_EBU_IFRS_DAY table created")

    cursor.execute(
        sql.SQL("""
        CREATE TABLE {} (
            ID_DATE NUMERIC,
            ID_HOUR NUMERIC,
            PRODUCT_LINE VARCHAR(100),
            SERVICE_PLAN VARCHAR(100),
            CUSTOMER_IDENTIFIER VARCHAR(200),
            APPLICATION_NAME VARCHAR(200),
            APPLICATION_FAMILY VARCHAR(200),
            LIVE_CONNECTIONS NUMERIC,
            NEW_CONNECTIONS NUMERIC,
            DL_MB NUMERIC,
            UL_MB NUMERIC,
            NETWORK_ACTIVITY_TIME_SEC NUMERIC,
            RTX_TCP_DATASEG_IN NUMERIC,
            TOTAL_TCP_DATASEG_IN NUMERIC
        )
        """).format(sql.Identifier("TEST_ALLOT_DATA_HOUR"))
    )
    print("[OK] TEST_ALLOT_DATA_HOUR table created")

    faker = Faker()
    Faker.seed(42)
    random.seed(42)

    insert_call_details = sql.SQL("""
    INSERT INTO {} (
        ID_DATE, START_TIME, END_TIME, TOTAL_CALL_DURATION, INTERACTION_ID,
        INTERACTION_RESOURCE_ID, SOURCE_ADDRESS, TARGET_ADDRESS, CONNID,
        CALLERPURPOSE, GVPEXITCODE, TECHNICAL_RESULT, RESULT_REASON,
        RESOURCE_ROLE, PLACE_NAME, RESOURCE_NAME, EMPLOYEE_ID, LANGUAGE,
        SUBSCRIBERTYPE, SUBSCRIBERVALUE, VB_THRESHOLD, VB_VERIFIED, SWITCH_NAME,
        ROUTING_POINT_DURATION, QUEUE_DURATION, RING_DURATION, TALK_DURATION,
        CUSTOMER_TALK_DURATION, HOLD_DURATION, AFTER_CALL_WORK_DURATION,
        MEDIA_NAME, INTERACTION_TYPE, SERVICE_SUBTYPE, VIRTUAL_QUEUE,
        DISPOSITION_CODE, HANDLE_COUNT, CUSTOMER_SEGMENT, IVR_SS_ROUTE, STOP_REASON
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """).format(sql.Identifier("TEST_INFORMAT_CALL_DETLS"))

    insert_ebu = sql.SQL("""
    INSERT INTO {} (
        ID_DATE, REVN_TYPE, REVN_SUB_TYPE, PRODUCT, ID_PDSV, NR_SBSC,
        SBSC_PAY_TYPE, ID_CST, CHILD_ACCT, PARENT_ACCT, PARENT_ACCT_NAME,
        EBU_CST_ID, EBU_HOLD_CST, ENTREPRISE_FLAG_OLD, EBU_CST_SGMN,
        EBU_CST_TRIBE, EBU_CST_SQUAD, EBU_CST_SECTOR, EBU_CST_REGION,
        GROSS_REVENUE, NET_REVENUE
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """).format(sql.Identifier("TEST_AGG_EBU_IFRS_DAY"))

    insert_allot = sql.SQL("""
    INSERT INTO {} (
        ID_DATE, ID_HOUR, PRODUCT_LINE, SERVICE_PLAN, CUSTOMER_IDENTIFIER,
        APPLICATION_NAME, APPLICATION_FAMILY, LIVE_CONNECTIONS, NEW_CONNECTIONS,
        DL_MB, UL_MB, NETWORK_ACTIVITY_TIME_SEC, RTX_TCP_DATASEG_IN,
        TOTAL_TCP_DATASEG_IN
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """).format(sql.Identifier("TEST_ALLOT_DATA_HOUR"))

    populate_table(
        cursor,
        "TEST_INFORMAT_CALL_DETLS",
        insert_call_details,
        lambda: generate_call_details_row(faker),
        TOTAL_ROWS,
        BATCH_SIZE,
    )
    populate_table(
        cursor,
        "TEST_AGG_EBU_IFRS_DAY",
        insert_ebu,
        lambda: generate_ebu_ifrs_row(faker),
        TOTAL_ROWS,
        BATCH_SIZE,
    )
    populate_table(
        cursor,
        "TEST_ALLOT_DATA_HOUR",
        insert_allot,
        generate_allot_data_row,
        TOTAL_ROWS,
        BATCH_SIZE,
    )

    for table in [
        "TEST_INFORMAT_CALL_DETLS",
        "TEST_AGG_EBU_IFRS_DAY",
        "TEST_ALLOT_DATA_HOUR",
    ]:
        cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}" ).format(sql.Identifier(table)))
        count = cursor.fetchone()[0]
        print(f"[OK] {table} contains {count:,} records")

    conn.close()
    print("\n" + "=" * 60)
    print("[OK] DATABASE INITIALIZATION COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    init_database()
