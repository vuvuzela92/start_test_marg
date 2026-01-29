from clickhouse_driver import Client
from typing import Optional, Union, List, Dict
import pandas as pd

class ClickHouseConnector:
    def __init__(
        self,
        host: str = None,
        port: int = 9000,
        user: str = None,
        password: str = None,
        database: str = None,
        settings: Optional[Dict] = None
    ):
        self.connection_params = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'settings': settings or {}
        }
        self.client = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def connect(self):
        try:
            self.client = Client(**self.connection_params)
            print("Успешное подключение к ClickHouse")
            return True
        except Exception as e:
            print(f"Ошибка подключения: {str(e)}")
            return False
    
    def close(self):
        if self.client:
            self.client.disconnect()
            print("Подключение к ClickHouse закрыто")
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict] = None,
        return_df: bool = False
    ) -> Union[List, pd.DataFrame, None]:
        if not self.client:
            self.connect()
        
        try:
            result = self.client.execute(query, params or {})
            
            if return_df:
                columns = [col[0] for col in self.client.last_query.columns]
                return pd.DataFrame(result, columns=columns)
            return result
        
        except Exception as e:
            print(f"Ошибка выполнения запроса: {str(e)}")
            return None
    
    def insert_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        chunk_size: int = 10000
    ) -> bool:
        if df.empty:
            print("DataFrame пустой, нечего вставлять")
            return False
        
        try:
            data = df.where(pd.notnull(df), None).values.tolist()
            
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                self.client.execute(
                    f"INSERT INTO {table_name} VALUES",
                    chunk,
                    types_check=True
                )
            
            print(f"Успешно вставлено {len(data)} строк в таблицу {table_name}")
            return True
        
        except Exception as e:
            print(f"Ошибка вставки данных: {str(e)}")
            return False