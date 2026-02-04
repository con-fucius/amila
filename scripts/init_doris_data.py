#!/usr/bin/env python3
"""
Doris Database Initialization Script
Creates and populates test tables for BI Agent testing.

Tables:
    - TEST_INFORMAT_CALL_DETLS (50,000 rows)
    - TEST_AGG_EBU_IFRS_DAY (50,000 rows)
    - TEST_ALLOT_DATA_HOUR (50,000 rows)

Usage:
    python scripts/init_doris_data.py

Prerequisites:
    - Doris container must be running and healthy
    - pip install pymysql faker
"""

import random
import sys
from datetime import datetime, timedelta
from typing import List, Tuple

try:
    import pymysql
except ImportError:
    print("ERROR: pymysql not installed. Run: pip install pymysql")
    sys.exit(1)

try:
    from faker import Faker
except ImportError:
    print("ERROR: faker not installed. Run: pip install faker")
    sys.exit(1)

# Configuration
import os
DORIS_HOST = os.getenv("DORIS_DB_HOST", "127.0.0.1")
DORIS_PORT = int(os.getenv("DORIS_DB_PORT", 9030))
DORIS_USER = os.getenv("DORIS_DB_USER", "root")
DORIS_PASSWORD = os.getenv("DORIS_DB_PASSWORD", "")
DORIS_DATABASE = os.getenv("DORIS_DB_DATABASE", "demo")

# Data generation constants
TOTAL_ROWS = 50000
BATCH_SIZE = 100
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


def wait_for_doris(max_retries: int = 60, retry_interval: int = 5) -> bool:
    """Wait for Doris to be ready with automatic localhost fallback."""
    import time
    import socket
    
    current_host = DORIS_HOST
    print(f"Waiting for Doris at {current_host}:{DORIS_PORT}...")
    
    for attempt in range(max_retries):
        try:
            conn = pymysql.connect(
                host=current_host,
                port=DORIS_PORT,
                user=DORIS_USER,
                password=DORIS_PASSWORD,
                connect_timeout=5,
            )
            conn.close()
            print(f"\n[OK] Doris is ready at {current_host}!")
            return True
        except Exception as e:
            error_msg = str(e)
            # If resolution fails (Errno 11001) or connection refused, try localhost
            if ("getaddrinfo failed" in error_msg or "Can't connect" in error_msg) and current_host != "127.0.0.1":
                if attempt == 0:
                    print(f"  Note: Could not resolve '{current_host}'. Falling back to 127.0.0.1 for local execution.")
                current_host = "127.0.0.1"
                
            if attempt < max_retries - 1:
                if attempt % 5 == 0:
                    print(f"  Attempt {attempt + 1}/{max_retries}: Waiting for Doris... ({error_msg[:100]})")
                else:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                time.sleep(retry_interval)
            else:
                print(f"\n[FAIL] Doris not ready after {max_retries} attempts")
                return False
    
    return False


def init_database():
    """Initialize the database and create tables."""
    print("\n" + "=" * 60)
    print("DORIS DATABASE INITIALIZATION")
    print("=" * 60)

    # Wait for Doris
    if not wait_for_doris():
        print("\nERROR: Doris is not available. Make sure the container is running:")
        print("  docker-compose up -d doris")
        print("  docker ps --filter 'name=doris'  # Should show 'healthy'")
        sys.exit(1)

    # Connect to Doris
    print("\nConnecting to Doris...")
    conn = pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        autocommit=True,
    )
    cursor = conn.cursor()

    # Create database
    print(f"Creating database '{DORIS_DATABASE}'...")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DORIS_DATABASE}")
    cursor.execute(f"USE {DORIS_DATABASE}")
    print(f"[OK] Database '{DORIS_DATABASE}' ready")

    print("Preparing test tables...")
    for table in [
        "TEST_INFORMAT_CALL_DETLS",
        "TEST_AGG_EBU_IFRS_DAY",
        "TEST_ALLOT_DATA_HOUR",
    ]:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")

    cursor.execute(
        """
        CREATE TABLE TEST_INFORMAT_CALL_DETLS (
            ID_DATE BIGINT,
            INTERACTION_ID BIGINT,
            START_TIME DATETIME,
            END_TIME DATETIME,
            TOTAL_CALL_DURATION BIGINT,
            INTERACTION_RESOURCE_ID BIGINT,
            SOURCE_ADDRESS VARCHAR(50),
            TARGET_ADDRESS VARCHAR(50),
            CONNID BIGINT,
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
            ROUTING_POINT_DURATION BIGINT,
            QUEUE_DURATION BIGINT,
            RING_DURATION BIGINT,
            TALK_DURATION BIGINT,
            CUSTOMER_TALK_DURATION BIGINT,
            HOLD_DURATION BIGINT,
            AFTER_CALL_WORK_DURATION BIGINT,
            MEDIA_NAME VARCHAR(50),
            INTERACTION_TYPE VARCHAR(50),
            SERVICE_SUBTYPE VARCHAR(50),
            VIRTUAL_QUEUE VARCHAR(150),
            DISPOSITION_CODE VARCHAR(50),
            HANDLE_COUNT BIGINT,
            CUSTOMER_SEGMENT VARCHAR(200),
            IVR_SS_ROUTE VARCHAR(255),
            STOP_REASON VARCHAR(255)
        ) ENGINE=OLAP
        DUPLICATE KEY(ID_DATE, INTERACTION_ID)
        DISTRIBUTED BY HASH(INTERACTION_ID) BUCKETS 8
        PROPERTIES (
            "replication_num" = "1"
        )
        """
    )
    print("[OK] TEST_INFORMAT_CALL_DETLS table created")

    cursor.execute(
        """
        CREATE TABLE TEST_AGG_EBU_IFRS_DAY (
            ID_DATE BIGINT NOT NULL,
            ID_PDSV BIGINT,
            REVN_TYPE VARCHAR(30),
            REVN_SUB_TYPE VARCHAR(150),
            PRODUCT VARCHAR(250),
            NR_SBSC VARCHAR(40),
            SBSC_PAY_TYPE VARCHAR(50),
            ID_CST BIGINT,
            CHILD_ACCT VARCHAR(30),
            PARENT_ACCT VARCHAR(50),
            PARENT_ACCT_NAME VARCHAR(300),
            EBU_CST_ID VARCHAR(800),
            EBU_HOLD_CST VARCHAR(800),
            ENTREPRISE_FLAG_OLD BIGINT,
            EBU_CST_SGMN VARCHAR(800),
            EBU_CST_TRIBE VARCHAR(800),
            EBU_CST_SQUAD VARCHAR(800),
            EBU_CST_SECTOR VARCHAR(800),
            EBU_CST_REGION CHAR(20),
            GROSS_REVENUE DOUBLE,
            NET_REVENUE DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(ID_DATE, ID_PDSV)
        DISTRIBUTED BY HASH(ID_PDSV) BUCKETS 8
        PROPERTIES (
            "replication_num" = "1"
        )
        """
    )
    print("[OK] TEST_AGG_EBU_IFRS_DAY table created")

    cursor.execute(
        """
        CREATE TABLE TEST_ALLOT_DATA_HOUR (
            ID_DATE BIGINT,
            ID_HOUR BIGINT,
            CUSTOMER_IDENTIFIER VARCHAR(200),
            PRODUCT_LINE VARCHAR(100),
            SERVICE_PLAN VARCHAR(100),
            APPLICATION_NAME VARCHAR(200),
            APPLICATION_FAMILY VARCHAR(200),
            LIVE_CONNECTIONS BIGINT,
            NEW_CONNECTIONS BIGINT,
            DL_MB DOUBLE,
            UL_MB DOUBLE,
            NETWORK_ACTIVITY_TIME_SEC BIGINT,
            RTX_TCP_DATASEG_IN BIGINT,
            TOTAL_TCP_DATASEG_IN BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(ID_DATE, ID_HOUR, CUSTOMER_IDENTIFIER)
        DISTRIBUTED BY HASH(CUSTOMER_IDENTIFIER) BUCKETS 8
        PROPERTIES (
            "replication_num" = "1"
        )
        """
    )
    print("[OK] TEST_ALLOT_DATA_HOUR table created")

    faker = Faker()
    Faker.seed(42)
    random.seed(42)

    insert_call_details = """
    INSERT INTO TEST_INFORMAT_CALL_DETLS (
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
    """

    insert_ebu = """
    INSERT INTO TEST_AGG_EBU_IFRS_DAY (
        ID_DATE, REVN_TYPE, REVN_SUB_TYPE, PRODUCT, ID_PDSV, NR_SBSC,
        SBSC_PAY_TYPE, ID_CST, CHILD_ACCT, PARENT_ACCT, PARENT_ACCT_NAME,
        EBU_CST_ID, EBU_HOLD_CST, ENTREPRISE_FLAG_OLD, EBU_CST_SGMN,
        EBU_CST_TRIBE, EBU_CST_SQUAD, EBU_CST_SECTOR, EBU_CST_REGION,
        GROSS_REVENUE, NET_REVENUE
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    insert_allot = """
    INSERT INTO TEST_ALLOT_DATA_HOUR (
        ID_DATE, ID_HOUR, PRODUCT_LINE, SERVICE_PLAN, CUSTOMER_IDENTIFIER,
        APPLICATION_NAME, APPLICATION_FAMILY, LIVE_CONNECTIONS, NEW_CONNECTIONS,
        DL_MB, UL_MB, NETWORK_ACTIVITY_TIME_SEC, RTX_TCP_DATASEG_IN,
        TOTAL_TCP_DATASEG_IN
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

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
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"[OK] {table} contains {count:,} records")

    conn.close()

    print("\n" + "=" * 60)
    print("[OK] DATABASE INITIALIZATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    init_database()
