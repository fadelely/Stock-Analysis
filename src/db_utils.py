from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database():
    try:
        conn = psycopg2.connect(
            host="pgdatabase",
            user="root",
            password="root",
            port=5432,
            dbname="postgres"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Check database existence
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'stockanalysis'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("CREATE DATABASE stockanalysis")
            print("Database created successfully")
        else:
            print("Database already exists")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error creating database: {e}")

def save_to_db(cleaned, table_name):
    create_database()
    engine = create_engine("postgresql://root:root@pgdatabase:5432/stockanalysis")

    try:
        with engine.connect() as conn:
            print("Connected to Database")
            print("Writing cleaned dataset to database")
            cleaned.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print("Done writing to database")
    except ValueError:
        print("Cleaned table already exists")
    except Exception as ex:
        print(f"Error saving to database: {ex}")
