import asyncio
import logging
import os
from app.core.postgres_client import postgres_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def apply_ddl():
    import psycopg
    from app.core.config import settings
    try:
        conninfo = (
            f"host={settings.POSTGRES_HOST} "
            f"port={settings.POSTGRES_PORT} "
            f"dbname={settings.POSTGRES_DATABASE} "
            f"user={settings.POSTGRES_USER} "
            f"password={settings.POSTGRES_PASSWORD}"
        )
        
        async with await psycopg.AsyncConnection.connect(conninfo) as conn:
            ddl_path = "scripts/create_audit_log.sql"
            with open(ddl_path, "r") as f:
                sql = f.read()
                
            async with conn.cursor() as cur:
                logger.info("Applying audit_log DDL...")
                await cur.execute(sql)
                await conn.commit()
                logger.info("Audit log table created successfully.")
                
    except Exception as e:
        logger.error(f"Failed to apply DDL: {e}")
    finally:
        await postgres_client.close()

if __name__ == "__main__":
    import selectors
    # psycopg3 requires SelectorEventLoop on Windows
    asyncio.run(apply_ddl(), loop_factory=asyncio.SelectorEventLoop)
