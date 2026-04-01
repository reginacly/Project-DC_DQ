import sqlalchemy as sa
from sqlalchemy.sql.elements import ColumnElement
if not hasattr(sa, "ColumnElement"):
    sa.ColumnElement = ColumnElement  # shim for GX
from trino.auth import BasicAuthentication
from abc import ABC, abstractmethod
from contextlib import contextmanager
import os
import pandas as pd
import psycopg2
import polars as pl

from odps import ODPS
from trino import dbapi as trino_dbapi
from trino.auth import BasicAuthentication
from trino.dbapi import Connection as TrinoConnection
from typing import Optional
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)
from typing import Any
warnings.filterwarnings("ignore", category=DeprecationWarning, module="odps.utils")
import pyodbc

class ConnectionHandler(ABC):
    @abstractmethod
    def connect(self, **kwargs):
        pass
    
    @abstractmethod
    def close(self) -> None:
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def execute_non_select_query(self, query: str) -> None:
        pass

class HologresConnection(ConnectionHandler):
    def __init__(self):
        self.conn = None

    def connect(self, **kwargs):
        self.conn = psycopg2.connect(
            host=get_env('endpoint_holo'),
            port=get_env('port_holo'),
            dbname=get_env('database_holo'),
            user=get_env('user_holo'),
            password=get_env('password_holo')
        )
        return self.conn

    def execute_query(self, query: str, values: Any = None) -> pl.DataFrame:
        if not self.conn or self.conn.closed:
            raise RuntimeError("Connection not established")
        try:
            with self.conn.cursor() as cursor:
                if values:
                    cursor.execute(query, values)
                else:
                    cursor.execute(query)

                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                data = cursor.fetchall() if cursor.description else []
                return pl.DataFrame(data, schema=columns, orient="row", strict=False)
        except psycopg2.Error as e:
            raise RuntimeError(str(e))


    def execute_non_select_query(self, query: str, values: Any = None) -> None:
        # print(query)
        if not self.conn or self.conn.closed:
            raise RuntimeError("Connection not established")
        try:
            with self.conn.cursor() as cursor:
                if values:
                    cursor.execute(query, values)
                else:
                    cursor.execute(query)
                self.conn.commit()
                print("Query executed successfully.")
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(str(e))

    def close(self) -> None:
        if self.conn and not self.conn.closed:
            self.conn.close()
            # print("PG connection closed")

class ODPSConnection(ConnectionHandler):
    def __init__(self):
        self.conn = None
        
    def connect(self, **kwargs):
        access_id = get_env("user_odps")
        secret = get_env("password_odps")
        project = get_env("project_odps")
        endpoint = get_env("endpoint_odps")

        if not project:
            raise RuntimeError("ENV project_odps not defined")

        self.conn = ODPS(
            access_id=access_id,
            secret_access_key=secret,
            project=project,
            endpoint=endpoint
        )

        return self.conn

    def execute_query(self, query: str, **kwargs) -> pd.DataFrame:

        if not self.conn:
            raise RuntimeError("Connection not established")

        q = query.strip().lower()

    # metadata query (DESC / SHOW) tidak boleh pakai script mode
        if q.startswith("desc") or q.startswith("show"):
            instance = self.conn.execute_sql(query)
        else:
            instance = self.conn.execute_sql(
                query,
                hints={"odps.sql.submit.mode": "script"}
            )

        instance.wait_for_completion()

        with instance.open_reader() as reader:
            return pl.from_pandas(reader.to_pandas())
        
    def execute_non_select_query(self, query):
        return super().execute_non_select_query(query)
            
    def close(self) -> None:
        return
        # print("ODPS connection closed")

class TrinoConnection(ConnectionHandler):
    def __init__(self):
        self.conn: Optional[TrinoConnection] = None

    def connect(self, **kwargs):
        user=get_env('user_datalake')
        password= get_env('password_datalake')
        self.conn = trino_dbapi.connect(
            host=get_env('endpoint_datalake'),
            port=get_env('port_datalake'),
            catalog=get_env('catalog_datalake'),
            schema=kwargs.get('schema_datalake'),
            auth=BasicAuthentication(user, password),
            http_scheme='https',
            verify=False
        )
        return self.conn

    def execute_query(self, query: str, values : Any = None) -> pd.DataFrame:
        # print(query)
        # print(values)
        if not self.conn:
            raise RuntimeError("Connection not established")

        else:
            try:
                cursor = self.conn.cursor()
                if values:
                    cursor.execute(query, values)
                else:
                    cursor.execute(query)
                rows = cursor.fetchall()
                headers = [desc[0] for desc in cursor.description]
                return pl.DataFrame(rows, schema=headers, orient="row")
            except Exception as e:
                raise RuntimeError(str(e))
        
    def execute_non_select_query(self, query: str, values : Any = None) -> None:
        # print(query)
        # print(values)
        if not self.conn:
            raise RuntimeError("Connection not established")
        else:
            try:
                cursor = self.conn.cursor()
                if values:
                    cursor.execute(query, values)
                else:
                    cursor.execute(query)
                print("Query executed successfully.")
            except Exception as e:
                raise RuntimeError(str(e))

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.close()
                # print("Trino connection closed")
            except Exception as e:
                raise (f"Error closing Trino connection: {str(e)}")
            
class SqlServerConnection(ConnectionHandler):
    def __init__(self):
        self.conn = None
    
    def connect(self, **kwargs):
        try:
            self.conn = pyodbc.connect(
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={get_env('server_dwh')};"
                f"DATABASE={get_env('database_dwh')};"
                "Trusted_Connection=yes;"
            )
            return self.conn
        except pyodbc.Error as e:
            raise RuntimeError(str(e))
    
    def execute_query(self, query: str) -> pl.DataFrame:
        try:
            with self.conn.cursor() as cursor:
                df = pd.read_sql_query(query, self.conn)
                return pl.from_pandas(df)
                        
        except pyodbc.Error as e:
            raise RuntimeError(str(e))
        
    def execute_non_select_query(self, query):
        return super().execute_non_select_query(query)
    
    def close(self):
        return self.conn.close()

class ConnectionFactory:
    @staticmethod
    def create_connection(stage: str) -> ConnectionHandler:
        if stage == 'postgres':
            return HologresConnection()
        if stage == 'odps':
            return ODPSConnection()
        if stage == 'trino':
            return TrinoConnection()
        if stage == 'dwh':
            return SqlServerConnection()
        else:
            raise ValueError(f"Unsupported database type: {stage}")

@contextmanager
def connection_manager(stage: str, **kwargs):
    connection = ConnectionFactory.create_connection(stage)
    connection.connect()
    try:
        yield connection
    finally:
        connection.close()

def get_env(key, default=None):
    return os.getenv(key, default)