import sqlite3
from sqlite3 import Error
import os
import json


class DBManager:
    def __init__(self, db_path, schema_path):
        print('DB Manager Startup')
        self.__db_path = db_path
        self.__schema_path = schema_path
        self.__verify_database()
        self.__connection = None
        self.__cursor = None

    def query(self, query):
        try:
            self.__connection = sqlite3.connect(self.__db_path)
            self.__connection.row_factory = sqlite3.Row
            self.__cursor = self.__connection.cursor()
            self.__cursor.execute(query)
        except Error as e:
            print(e)

    def end_query(self):
        try:
            self.__connection.commit()
            self.__connection.close()
        except Error as e:
            print(e)

    def get_cursor(self):
         return self.__cursor

    def get_all(self, query):
        try:
           self.query(query)
           cursor = self.get_cursor()
           response = cursor.fetchall()
           self.end_query()
           return response
        except:
            print("unable to get")

    def add_entry(self, tableName, entry):
        """ Expects an entry of type
            {
                'data': [
                    { 'fieldName': '', 'value': '' },
                    { 'fieldName': '', 'value': '' }
                ]
            }
        """
        data = entry['data']
        fieldNames = []
        values = []
        for field in data:
            fieldNames.append(field['fieldName'])
            values.append(field['value'])
        fieldNamesString = ', '.join(fieldNames) 
        valuesQueryString = ', '.join(list(map(lambda a: '"{0}"'.format(a), values)))  
        print(fieldNamesString)
        print(valuesQueryString)
        sql_query = """ INSERT into {0}({1})
        VALUES({2});""".format(tableName, fieldNamesString, valuesQueryString)
        self.query(sql_query)
        self.end_query()

    def __verify_database(self):
        print('Verifying database')
        # Check if DB File Exists.
        try:
            if(not os.path.exists(self.__db_path)):
                print('  - unable to find database file')
                # Load & Parse Tables from JSON
                schema = self.__load_schema();
                self.__create_database([schema])
            else:
                # Attempt to make connection
                self.__connection = sqlite3.connect(self.__db_path)
                print('  - database initialised')
        except Error as e:
            raise e
        finally: 
            if self.__connection: self.__connection.close()

    def __create_database(self, table_queries):
        try:
            print('  - creating database {0}'.format(self.__db_path))
            path = self.__db_path[:-10]
            if(not os.path.exists(path)):
                print('    - could not find path \
                        {0}'.format(path))
                os.mkdir(path)

            for table_query in table_queries:
                sql_query = self.__create_table_query(table_query)
                print(sql_query)
                self.query(sql_query)
                self.end_query()
        except Error as e:
            raise e

    def __create_table_query(self, table):
        table_name = table['table_name']
        fields = table['fields']
        aggregateFieldStrings = ""
        for field in fields:
            aggregateFieldStrings = aggregateFieldStrings + '{0} {1} \
                    {2}{3},\n'.format(
                        field['fname'],
                        field['ftype'],
                        field['fprimary'],
                        field['isnullable'])
            if(fields[-1] == field):
                aggregateFieldStrings = aggregateFieldStrings[:-2]
                + aggregateFieldStrings[-1]
        query_string = """CREATE TABLE IF NOT EXISTS {0} (
            {1}
        );""".format(table_name, aggregateFieldStrings)
        return query_string

    def __load_schema(self):
        try:
            with open(self.__schema_path) as f:
                data = json.load(f)
                return data['tables'][0]
        except Error as e:
            print('Error: Unable to load JSON file')
            raise e
