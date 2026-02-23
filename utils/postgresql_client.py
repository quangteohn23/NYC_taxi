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
        # lấy danh sách tên cột của 1 bảng
        # engine = create_engine(
        #     f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}"
        # )
        # conn = engine.connect()
        # df = pd.read_sql(f"select * from {table_name}", conn)
        # return df.columns

        query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name.split('.')[-1]}'
            ORDER BY ordinal_position
        """
        engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

        with engine.connect() as conn:
            df = pd.read_sql(query,conn)
        
        return df["column_name"].tolist()