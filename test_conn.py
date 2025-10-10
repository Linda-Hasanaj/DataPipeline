# test_conn.py
from sqlalchemy import create_engine, text

dsn = "postgresql+psycopg://postgres:Attributy123!@localhost:5432/Pipeline"

try:
    engine = create_engine(dsn)
    with engine.begin() as conn:
        result = conn.execute(text("SELECT 1")).scalar_one()
        print("Connected OK, result:", result)
    engine.dispose()
except Exception as e:
    print("Connection failed:", e)
