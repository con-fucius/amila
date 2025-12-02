import asyncio
import logging
import os
import subprocess
import time
import random
import string
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DorisSetupManager:
    def __init__(self):
        self.work_dir = Path("doris_setup")
        self.work_dir.mkdir(exist_ok=True)
        self.fe_port = 9030
        self.be_port = 8040
        self.fe_http_port = 8030
        self.root_password = "" # Default empty for quickstart
        self.container_name = "doris_quickstart"

    async def setup_local_doris(self):
        """
        Sets up a local Apache Doris instance using the official quickstart script/image.
        """
        logger.info(" Setting up local Apache Doris environment...")
        
        # check if docker is installed
        try:
            subprocess.run(["docker", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error(" Docker is not installed or not in PATH. Please install Docker Desktop.")
            return False

        # Check if container already exists
        check_container = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name={self.container_name}", "--format", "{{.Names}}"],
            capture_output=True, text=True
        )
        
        if self.container_name in check_container.stdout:
            logger.info(f" Container {self.container_name} already exists.")
            # Check if running
            running_check = subprocess.run(
                ["docker", "ps", "--filter", f"name={self.container_name}", "--format", "{{.Names}}"],
                capture_output=True, text=True
            )
            if self.container_name not in running_check.stdout:
                logger.info(" Starting existing container...")
                subprocess.run(["docker", "start", self.container_name], check=True)
        else:
            logger.info(" Pulling and starting Apache Doris Docker image (apache/doris:doris-all-in-one-2.1.0)...")
            # Using the all-in-one image which has FE and BE in one container
            cmd = [
                "docker", "run", "-d",
                "--name", self.container_name,
                "-p", f"{self.fe_port}:{self.fe_port}",
                "-p", f"{self.fe_http_port}:{self.fe_http_port}",
                "-p", f"{self.be_port}:{self.be_port}",
                "apache/doris:doris-all-in-one-2.1.0" 
            ]
            subprocess.run(cmd, check=True)
            
        logger.info(" Waiting for Doris to become ready (this may take 30-60s)...")
        # Simple wait loop
        for i in range(60):
            try:
                # Try to connect with mysql client via docker exec if local mysql not available
                # Or just check logs
                result = subprocess.run(
                    ["docker", "exec", self.container_name, "mysql", "-uroot", f"-P{self.fe_port}", "-h127.0.0.1", "-e", "SELECT 1"],
                    capture_output=True
                )
                if result.returncode == 0:
                    logger.info(" Doris is ready!")
                    break
            except Exception:
                pass
            time.sleep(2)
            if i % 10 == 0:
                logger.info(f"   Waiting... ({i}s)")
        else:
            logger.warning(" Timed out waiting for Doris. It might still be starting.")

        return True

    async def create_test_user(self):
        """Creates a test user and assigns permissions."""
        logger.info(" Creating test user 'test_user'...")
        
        sql_commands = [
            "CREATE USER IF NOT EXISTS 'test_user'@'%' IDENTIFIED BY 'test_pass';",
            "GRANT ALL ON *.* TO 'test_user'@'%';", # Grant all for dev simplicity
        ]
        
        for sql in sql_commands:
            self._run_sql(sql)
            
        logger.info(" User 'test_user' created with password 'test_pass'.")

    async def create_dummy_data(self):
        """Creates a dummy table and loads 1 million rows."""
        logger.info(" Creating dummy data...")
        
        # 1. Create Database
        self._run_sql("CREATE DATABASE IF NOT EXISTS demo;")
        
        # 2. Create Table (using simpler approach)
        create_table_sql = """CREATE TABLE IF NOT EXISTS demo.user_behavior (
user_id BIGINT,
item_id BIGINT,
category_id BIGINT,
behavior_type VARCHAR(20),
timestamp DATETIME
) UNIQUE KEY(user_id, item_id, category_id, behavior_type, timestamp) DISTRIBUTED BY HASH(user_id) BUCKETS 10 PROPERTIES("replication_num" = "1");"""
        
        self._run_sql(create_table_sql)
            
        # 2. Generate Dummy CSV
        csv_path = self.work_dir / "dummy_data.csv"
        if not csv_path.exists():
            logger.info(" Generating 1 million rows of CSV data (this may take a moment)...")
            with open(csv_path, "w") as f:
                # Header not needed for stream load usually, but good for ref
                # f.write("user_id,item_id,category_id,behavior_type,timestamp\n")
                
                # Generate in chunks for speed
                batch_size = 10000
                total_rows = 1000000
                behaviors = ["pv", "buy", "cart", "fav"]
                
                for i in range(0, total_rows, batch_size):
                    lines = []
                    for _ in range(batch_size):
                        uid = random.randint(1, 1000000)
                        iid = random.randint(1, 100000)
                        cid = random.randint(1, 5000)
                        btype = random.choice(behaviors)
                        ts = "2025-11-20 10:00:00"
                        lines.append(f"{uid},{iid},{cid},{btype},{ts}\n")
                    f.write("".join(lines))
                    if i % 100000 == 0:
                        logger.info(f"   Generated {i} rows...")
                        
            logger.info(f" CSV generated at {csv_path}")

        # 3. Load Data via Stream Load (curl)
        logger.info(" Loading data into Doris...")
        
        # Construct curl command for Stream Load
        # curl --location-trusted -u root: -T dummy_data.csv -H "column_separator:," http://127.0.0.1:8030/api/demo/user_behavior/_stream_load
        
        cmd = [
            "curl", "--location-trusted", 
            "-u", "root:", 
            "-T", str(csv_path),
            "-H", "column_separator:,",
            f"http://127.0.0.1:{self.fe_http_port}/api/demo/user_behavior/_stream_load"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if '"Status": "Success"' in result.stdout:
                logger.info(" Data loaded successfully!")
                logger.info(f"   Response: {result.stdout}")
            else:
                logger.error(f" Data load failed: {result.stdout}")
        except Exception as e:
            logger.error(f" Failed to run stream load: {e}")

    def _run_sql(self, sql):
        """Helper to run SQL inside the container."""
        cmd = [
            "docker", "exec", self.container_name, 
            "mysql", "-uroot", f"-P{self.fe_port}", "-h127.0.0.1", 
            "-e", sql
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"SQL execution failed: {result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)

async def main():
    manager = DorisSetupManager()
    if await manager.setup_local_doris():
        await manager.create_test_user()
        await manager.create_dummy_data()
        
        print("\n Setup Complete!")
        print("Connection Details:")
        print(f"  Host: 127.0.0.1")
        print(f"  Port: {manager.fe_port}")
        print(f"  User: test_user")
        print(f"  Pass: test_pass")
        print(f"  DB:   demo")
        print("\nUpdate your .env with these credentials.")

if __name__ == "__main__":
    asyncio.run(main())
