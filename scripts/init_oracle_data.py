#!/usr/bin/env python3
"""
Oracle Database Initialization Script
Creates and populates test tables for BI Agent testing.

Tables:
    - TEST_INFORMAT_CALL_DETLS (50,000 rows)
    - TEST_AGG_EBU_IFRS_DAY (50,000 rows)
    - TEST_ALLOT_DATA_HOUR (50,000 rows)

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


def populate_table(cursor, conn, table_name: str, insert_sql: str, row_fn, total_rows: int, batch_size: int):
    print(f"\nPopulating {table_name} with {total_rows:,} rows...")
    inserted = 0
    while inserted < total_rows:
        current_batch = min(batch_size, total_rows - inserted)
        batch = [row_fn() for _ in range(current_batch)]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        inserted += current_batch
        if inserted % (batch_size * 5) == 0 or inserted == total_rows:
            print(f"  Inserted {inserted:,} / {total_rows:,} rows into {table_name}...")

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
            
            # CRITICAL: Stop immediately on authentication errors to avoid account lock
            if "ORA-01017" in error_msg:
                print(f"\n[FAIL] Oracle Login Denied: Invalid credentials for user '{ORACLE_USER}'.")
                print(f"Check ORACLE_PASSWORD in .env or shell environment.")
                return False
            
            if "ORA-28000" in error_msg:
                print(f"\n[FAIL] Oracle Account Locked: User '{ORACLE_USER}' is locked.")
                print("Unlock with: docker exec -it bi-agent-oracle ./unlock_system.sh (or equivalent SQL).")
                return False

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
    """Initialize the database and create tables."""
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

    def drop_table(table_name: str):
        try:
            cursor.execute(f"DROP TABLE {table_name}")
        except oracledb.DatabaseError as e:
            error, = e.args
            if error.code != 942:  # ORA-00942: table or view does not exist
                raise

    print("Preparing test tables...")
    for table in [
        "TEST_INFORMAT_CALL_DETLS",
        "TEST_AGG_EBU_IFRS_DAY",
        "TEST_ALLOT_DATA_HOUR",
    ]:
        drop_table(table)

    cursor.execute(
        """
        CREATE TABLE TEST_INFORMAT_CALL_DETLS (
            ID_DATE NUMBER(22),
            START_TIME TIMESTAMP(6),
            END_TIME TIMESTAMP(6),
            TOTAL_CALL_DURATION NUMBER(22),
            INTERACTION_ID NUMBER(22),
            INTERACTION_RESOURCE_ID NUMBER(22),
            SOURCE_ADDRESS VARCHAR2(50),
            TARGET_ADDRESS VARCHAR2(50),
            CONNID NUMBER(22),
            CALLERPURPOSE VARCHAR2(50),
            GVPEXITCODE VARCHAR2(50),
            TECHNICAL_RESULT VARCHAR2(50),
            RESULT_REASON VARCHAR2(50),
            RESOURCE_ROLE VARCHAR2(50),
            PLACE_NAME VARCHAR2(50),
            RESOURCE_NAME VARCHAR2(50),
            EMPLOYEE_ID VARCHAR2(50),
            LANGUAGE VARCHAR2(50),
            SUBSCRIBERTYPE VARCHAR2(50),
            SUBSCRIBERVALUE VARCHAR2(50),
            VB_THRESHOLD VARCHAR2(50),
            VB_VERIFIED VARCHAR2(50),
            SWITCH_NAME VARCHAR2(50),
            ROUTING_POINT_DURATION NUMBER(22),
            QUEUE_DURATION NUMBER(22),
            RING_DURATION NUMBER(22),
            TALK_DURATION NUMBER(22),
            CUSTOMER_TALK_DURATION NUMBER(22),
            HOLD_DURATION NUMBER(22),
            AFTER_CALL_WORK_DURATION NUMBER(22),
            MEDIA_NAME VARCHAR2(50),
            INTERACTION_TYPE VARCHAR2(50),
            SERVICE_SUBTYPE VARCHAR2(50),
            VIRTUAL_QUEUE VARCHAR2(150),
            DISPOSITION_CODE VARCHAR2(50),
            HANDLE_COUNT NUMBER(22),
            CUSTOMER_SEGMENT VARCHAR2(200),
            IVR_SS_ROUTE VARCHAR2(255),
            STOP_REASON VARCHAR2(255)
        )
        """
    )
    print("[OK] TEST_INFORMAT_CALL_DETLS table created")

    cursor.execute(
        """
        CREATE TABLE TEST_AGG_EBU_IFRS_DAY (
            ID_DATE NUMBER(8, 0) NOT NULL,
            REVN_TYPE VARCHAR2(30),
            REVN_SUB_TYPE VARCHAR2(150),
            PRODUCT VARCHAR2(250),
            ID_PDSV NUMBER,
            NR_SBSC VARCHAR2(40),
            SBSC_PAY_TYPE VARCHAR2(50),
            ID_CST NUMBER,
            CHILD_ACCT VARCHAR2(30),
            PARENT_ACCT VARCHAR2(50),
            PARENT_ACCT_NAME VARCHAR2(300),
            EBU_CST_ID VARCHAR2(800),
            EBU_HOLD_CST VARCHAR2(800),
            ENTREPRISE_FLAG_OLD NUMBER,
            EBU_CST_SGMN VARCHAR2(800),
            EBU_CST_TRIBE VARCHAR2(800),
            EBU_CST_SQUAD VARCHAR2(800),
            EBU_CST_SECTOR VARCHAR2(800),
            EBU_CST_REGION CHAR(20),
            GROSS_REVENUE NUMBER,
            NET_REVENUE NUMBER
        )
        """
    )
    print("[OK] TEST_AGG_EBU_IFRS_DAY table created")

    cursor.execute(
        """
        CREATE TABLE TEST_ALLOT_DATA_HOUR (
            ID_DATE NUMBER,
            ID_HOUR NUMBER,
            PRODUCT_LINE VARCHAR2(100),
            SERVICE_PLAN VARCHAR2(100),
            CUSTOMER_IDENTIFIER VARCHAR2(200),
            APPLICATION_NAME VARCHAR2(200),
            APPLICATION_FAMILY VARCHAR2(200),
            LIVE_CONNECTIONS NUMBER,
            NEW_CONNECTIONS NUMBER,
            DL_MB NUMBER,
            UL_MB NUMBER,
            NETWORK_ACTIVITY_TIME_SEC NUMBER,
            RTX_TCP_DATASEG_IN NUMBER,
            TOTAL_TCP_DATASEG_IN NUMBER
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
        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
        :11, :12, :13, :14, :15, :16, :17, :18, :19, :20,
        :21, :22, :23, :24, :25, :26, :27, :28, :29, :30,
        :31, :32, :33, :34, :35, :36, :37, :38, :39
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
        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
        :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21
    )
    """

    insert_allot = """
    INSERT INTO TEST_ALLOT_DATA_HOUR (
        ID_DATE, ID_HOUR, PRODUCT_LINE, SERVICE_PLAN, CUSTOMER_IDENTIFIER,
        APPLICATION_NAME, APPLICATION_FAMILY, LIVE_CONNECTIONS, NEW_CONNECTIONS,
        DL_MB, UL_MB, NETWORK_ACTIVITY_TIME_SEC, RTX_TCP_DATASEG_IN,
        TOTAL_TCP_DATASEG_IN
    ) VALUES (
        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14
    )
    """

    populate_table(
        cursor,
        conn,
        "TEST_INFORMAT_CALL_DETLS",
        insert_call_details,
        lambda: generate_call_details_row(faker),
        TOTAL_ROWS,
        BATCH_SIZE,
    )
    populate_table(
        cursor,
        conn,
        "TEST_AGG_EBU_IFRS_DAY",
        insert_ebu,
        lambda: generate_ebu_ifrs_row(faker),
        TOTAL_ROWS,
        BATCH_SIZE,
    )
    populate_table(
        cursor,
        conn,
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
    print("[OK] ORACLE INITIALIZATION COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    init_database()
