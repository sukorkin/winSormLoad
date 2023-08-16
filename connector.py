import oracledb
import pandas as pd
import config


class Connector:
    '''создаем класс для работы с БД'''
    def __init__(self):
        '''use your name,pass,host,port and service name to connect db'''
        self.cursor = None
        self.connection = None
        self.user = config.username
        self.password = config.password
        self.host = config.host
        self.port = config.port
        self.service_name = config.service_name
        self.arraysize = config.arraysize

    def create_connection(self):
        '''this method create connection using init parametrs'''
        # dsn_tns = cx_Oracle.makedsn(self.host, self.port, service_name=self.service_name)
        dsn = f'{self.user}/{self.password}@{self.host}:{self.port}/{self.service_name}'
        self.connection = oracledb.connect(dsn=dsn)
        self.cursor = self.connection.cursor()
        self.cursor.arraysize = self.arraysize
        print('-- Connection successful! --')

    def get_cursor(self, query):
        cursor = self.cursor.execute(query)
        print('-- Data received successfully! --')
        return cursor

    def get_query(self, query):
        '''this method return query from db in format DataFrame'''
        info = self.cursor.execute(query)
        data = info.fetchall()
        columns = [column[0] for column in info.description]
        df = pd.DataFrame(data, columns=columns)
        print('-- Data received successfully! --')
        return df
