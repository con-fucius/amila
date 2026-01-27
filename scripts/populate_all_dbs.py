#!/usr/bin/env python3
"""
Unified Database Provisioner & Cleaner
Manages population and cleanup of Oracle, Doris, and Postgres databases.

Usage:
    python scripts/populate_all_dbs.py [--clean] [--reset]
    
    Default (no args): Populates databases (skips if tables exist/fails, depending on init script)
    --clean: Drops tables from all databases
    --reset: Drops tables and then repopulates (Fresh Start)
"""

import sys
import os
import logging
import argparse
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("UnifiedDB")

# Load environment variables
load_dotenv()

# ==========================================
# CLEANUP FUNCTIONS
# ==========================================

def clean_oracle():
    try:
        import oracledb
        logger.info("Cleaning Oracle...")
        host = os.getenv("ORACLE_HOST", "127.0.0.1")
        port = os.getenv("ORACLE_PORT", "1521")
        user = os.getenv("ORACLE_USERNAME", "system")
        pwd = os.getenv("ORACLE_PASSWORD", "password")
        svc = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")
        
        conn = oracledb.connect(user=user, password=pwd, dsn=f"{host}:{port}/{svc}")
        cursor = conn.cursor()
        try:
            cursor.execute("DROP TABLE CUSTOMER_DATA")
            logger.info("Oracle: Dropped CUSTOMER_DATA")
        except oracledb.DatabaseError as e:
            error, = e.args
            if error.code == 942: # Table doesn't exist
                logger.info("Oracle: Table did not exist")
            else:
                logger.error(f"Oracle Error: {e}")
        conn.close()
        return True
    except ImportError:
        logger.error("Oracle cleanup skipped: oracledb module not found")
        return False
    except Exception as e:
        logger.error(f"Oracle cleanup failed: {e}")
        return False

def clean_doris():
    try:
        import pymysql
        logger.info("Cleaning Doris...")
        host = os.getenv("DORIS_DB_HOST", "127.0.0.1")
        port = int(os.getenv("DORIS_DB_PORT", 9030))
        user = os.getenv("DORIS_DB_USER", "root")
        pwd = os.getenv("DORIS_DB_PASSWORD", "")
        db = os.getenv("DORIS_DB_DATABASE", "demo")
        
        conn = pymysql.connect(host=host, port=port, user=user, password=pwd, database=db)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS CUSTOMER_DATA")
        logger.info("Doris: Dropped CUSTOMER_DATA")
        conn.close()
        return True
    except ImportError:
        logger.error("Doris cleanup skipped: pymysql module not found")
        return False
    except Exception as e:
        logger.error(f"Doris cleanup failed: {e}")
        return False

def clean_postgres():
    try:
        # Try importing psycopg (v3) or psycopg2 (v2)
        try:
            import psycopg
            conn_func = lambda: psycopg.connect(
                host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                dbname=os.getenv("POSTGRES_DATABASE", "postgres"),
                autocommit=True
            )
        except ImportError:
            try:
                import psycopg2
                conn_func = lambda: psycopg2.connect(
                    host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
                    port=os.getenv("POSTGRES_PORT", "5432"),
                    user=os.getenv("POSTGRES_USER", "postgres"),
                    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                    dbname=os.getenv("POSTGRES_DATABASE", "postgres")
                )
            except ImportError:
                logger.error("Postgres cleanup skipped: neither 'psycopg' nor 'psycopg2' module found")
                return False

        logger.info("Cleaning Postgres...")
        conn = conn_func()
        if hasattr(conn, "autocommit"): # psycopg2 requires manual autocommit set
            conn.autocommit = True
            
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS CUSTOMER_DATA")
        logger.info("Postgres: Dropped CUSTOMER_DATA")
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Postgres cleanup failed: {e}")
        return False

# ==========================================
# POPULATE FUNCTIONS
# ==========================================

def run_init(module_name, func_name="init_database"):
    """Dynamically import and run the init function from a script."""
    try:
        # Add scripts dir to path if not present
        scripts_dir = os.path.dirname(os.path.abspath(__file__))
        if scripts_dir not in sys.path:
            sys.path.append(scripts_dir)
            
        # Import the module
        if module_name == "init_postgres_data":
            from init_postgres_data import init_database as init_fn
        elif module_name == "init_doris_data":
            from init_doris_data import init_database as init_fn
        elif module_name == "init_oracle_data":
            from init_oracle_data import init_database as init_fn
        else:
            logger.error(f"Unknown module: {module_name}")
            return False

        # Run the initialization
        init_fn()
        return True
    except ImportError as e:
        logger.error(f"Failed to import {module_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"FAILURE: {module_name} failed: {e}")
        return False

# ==========================================
# MAIN
# ==========================================

def main():
    parser = argparse.ArgumentParser(description="Unified Database Provisioner & Cleaner")
    parser.add_argument("--clean", action="store_true", help="Only drop tables")
    parser.add_argument("--reset", action="store_true", help="Drop tables then repopulate")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print(" UNIFIED DATABASE MANAGER")
    print("=" * 60)
    
    # Mode Logic
    do_clean = args.clean or args.reset
    do_populate = (not args.clean) # Default is populate, unless --clean only is set. --reset does both.
    
    results = {}

    if do_clean:
        print("\n--- PHASE 1: CLEANUP ---")
        results["Oracle Cleanup"] = clean_oracle()
        results["Doris Cleanup"] = clean_doris()
        results["Postgres Cleanup"] = clean_postgres()

    if do_populate:
        print("\n--- PHASE 2: POPULATION ---")
        # 1. Oracle
        results["Oracle Init"] = run_init("init_oracle_data")
        
        # 2. Doris
        results["Doris Init"] = run_init("init_doris_data")
        
        # 3. Postgres
        results["Postgres Init"] = run_init("init_postgres_data")
    
    # Final Summary
    print("\n" + "=" * 60)
    print(" EXECUTION SUMMARY")
    print("=" * 60)
    
    # Group results by DB
    dbs = ["Oracle", "Doris", "Postgres"]
    all_success = True
    
    for db in dbs:
        clean_status = results.get(f"{db} Cleanup")
        init_status = results.get(f"{db} Init")
        
        status_parts = []
        if clean_status is not None:
            status_parts.append(f"Clean: {'✅' if clean_status else '❌'}")
        if init_status is not None:
            status_parts.append(f"Init: {'✅' if init_status else '❌'}")
            
        print(f"{db:10}: {', '.join(status_parts)}")
        
        if clean_status is False or init_status is False:
            all_success = False

    print("=" * 60 + "\n")
    
    if all_success:
        print("Operation completed successfully.")
    else:
        print("Some operations failed. Check logs above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
