from abc import ABC, abstractmethod
from contextlib import contextmanager
import os
import pandas as pd
import psycopg2

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
import pandas as pd

class ConnectionHandler(ABC):
    @abstractmethod
    def connect(self, config: dict[str, Any], **kwargs):
        pass
    
    @abstractmethod
    def close(self) -> None:
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        pass

    def execute_non_select_query(self, query: str) -> None:
        pass

class PostgresConnection(ConnectionHandler):
    def __init__(self):
        self.conn = None

    def connect(self, config: dict[str, Any], **kwargs):
        self.conn = psycopg2.connect(
            host=config.get('endpoint'),
            port=config.get('port'),
            dbname=config.get('dbname'),
            user=config.get('user'),
            password=config.get('password')
        )
        return self.conn

    def execute_query(self, query: str) -> pd.DataFrame:
        if not self.conn or self.conn.closed:
            raise RuntimeError("Connection not established")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
        except psycopg2.Error as e:
            raise RuntimeError(str(e))

    def execute_non_select_query(self, query: str) -> None:
        if not self.conn or self.conn.closed:
            raise RuntimeError("Connection not established")

        try:
            with self.conn.cursor() as cursor:
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
        
    def connect(self, config: dict[str, Any], **kwargs):
        self.conn = ODPS(
            access_id = config.get('user'),
            secret_access_key = config.get('password'),
            project = config.get('project'),
            endpoint = config.get('endpoint')
        )
        return self.conn

    def execute_query(self, query: str, **kwargs) -> pd.DataFrame:
        if not self.conn:
            raise RuntimeError("Connection not established")
        
        instance = self.conn.execute_sql(query, hints={ "odps.sql.submit.mode" : "script"})
        instance.wait_for_completion()
        with instance.open_reader() as reader:
            return reader.to_pandas()
        
    def execute_non_select_query(self, query: str) -> None:
        if not self.conn:
            raise RuntimeError("Connection not established")

        instance = self.conn.execute_sql(query)
        instance.wait_for_completion()
        # print("Query executed successfully.")
            
    def close(self) -> None:
        return
        # print("ODPS connection closed")

class TrinoConnection(ConnectionHandler):
    def __init__(self):
        self.conn: Optional[TrinoConnection] = None

    def connect(self, config: dict[str, Any], **kwargs):
        user=config.get('user')
        password= config.get('password')
        self.conn = trino_dbapi.connect(
            host=config.get('endpoint'),
            port=config.get('port'),
            catalog=config.get('catalog', 'hive'),
            schema=kwargs.get('schema', 'wsbfi_3_dev'),
            auth=BasicAuthentication(user, password),
            http_scheme='https',
            verify=False
        )
        return self.conn

    def execute_query(self, query: str) -> pd.DataFrame:
        if not self.conn:
            raise RuntimeError("Connection not established")

        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=headers)
        
    def execute_non_select_query(self, query: str) -> None:
        if not self.conn:
            raise RuntimeError("Connection not established")

        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            print("Query executed successfully.")
        except Exception as e:
           raise RuntimeError(str(e))


    def close(self) -> None:
        if self.conn:
            try:
                self.conn.close()
                print("Trino connection closed")
            except Exception as e:
                raise (f"Error closing Trino connection: {str(e)}")
            
class SqlServerConnection(ConnectionHandler):
    def __init__(self):
        self.conn = None
    
    def connect(self, config, **kwargs):
        return super().connect(config, **kwargs)
    
    def execute_query(self, query):
        return super().execute_query(query)
    
    def execute_non_select_query(self, query):
        return super().execute_non_select_query(query)
    
    def close(self):
        return super().close()

class ConnectionFactory:
    @staticmethod
    def create_connection(stage: str) -> ConnectionHandler:
        if stage == 'pg':
            return PostgresConnection()
        if stage == 'holo':
            return PostgresConnection()
        if stage == 'odps':
            return ODPSConnection()
        if stage == 'datalake':
            return TrinoConnection()
        if stage == 'dwh':
            return SqlServerConnection()
        else:
            raise ValueError(f"Unsupported database type: {stage}")

@contextmanager
def connection_manager(stage: str, config: dict[str, Any], **kwargs):
    connection = ConnectionFactory.create_connection(stage)
    connection.connect(config)
    try:
        yield connection
    finally:
        connection.close()

def get_env(key, default=None):
        return os.getenv(key, default)

def get_connection_info(stage):
    if stage == "pg":
        user = get_env("user_pg")
        password = get_env("password_pg")
    if stage in {"holo", "odps"}:
        user = get_env("shared_user")
        password = get_env("shared_password")
    elif stage == "datalake":
        user = get_env("user_datalake")
        password = get_env("password_datalake")
    elif stage == "dwh":
        user = None
        password = None
    else:
        raise ValueError(f"Unsupported stage type: {stage}")

    connection = {
        "stage": stage,
        "user": user,
        "password": password,
        "endpoint": get_env(f"endpoint_{stage}"),
        "port": get_env(f"port_{stage}"),
    }

    if stage == "holo":
        connection["dbname"] = get_env("holo_dbname")
    if stage == "pg":
        connection["dbname"] = get_env("pg_dbname")
    if stage == "odps":
        connection["project"] = get_env("project_odps")
    if stage == "dwh":
        connection["dbname"] = get_env("dwh_dbname")

    return connection


