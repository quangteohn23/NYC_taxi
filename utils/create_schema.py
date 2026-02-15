import os
from dotenv import load_dotenv
load_dotenv(".env")

def main():

    pc = PostgresSQLClient(
        database = os.getenv("POSTGRES_DB"),
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD")
    )

    try:
        pc.execute(create_iot_schema)
        pc.execute(create_staging_schema)
        pc.execute(create_production_schema)
    except Exception as e:
        print(f"Failed to create schema: {e}")

if __name__ == "__main__":
    main()
