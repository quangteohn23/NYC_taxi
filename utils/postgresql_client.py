import pandas as pd
import psycopg2
from sqlalchemy import create_engine


class PostgresSQLClient:
    # hàm khởi tạo thông tin kết nối
    def __init__(self, database, user, password, host="localhost", port="5432"):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def create_conn(self): # tạo kết nối tới PostgreSQL
        conn = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        return conn

    def execute_query(self, query): # thực thi 1 câu sql bất kỳ
        conn = self.create_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()
            print("Query executed successfully!")
        except Exception as e:
            conn.rollback()
            print(f"Error: {e}")
        finally:
            conn.close()


    def get_columns(self, table_name):
        pure_table_name = table_name.split('.')[-1]
        
        query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{pure_table_name}'
            ORDER BY ordinal_position
        """
        conn = self.create_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                columns = [row[0] for row in cursor.fetchall()]
            return columns
        finally:
            conn.close()