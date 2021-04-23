import MySQLdb

DB = ""
HOST = "localhost"
USER = "root"
PASSWORD = ""
PORT = 3306


class DataBase:

    def create_connection_and_cursor(self, db_name: str = "") -> None:
        self.conn = MySQLdb.connect(host=HOST, user=USER, password=PASSWORD, port=PORT, db=db_name)
        self.conn.autocommit(True)
        self.cursor = self.conn.cursor()

    def conn_and_cursor_exist(self) -> bool:
        try:
            self.conn
            self.cursor
            return True
        except AttributeError:
            return False

    def is_database_selected(self) -> bool:
        try:
            self.cursor.execute("CREATE TABLE temp_table (teste varchar(1))")
            self.cursor.execute("DROP TABLE temp_table")
            return True
        except Exception:
            return False

    def change_current_database(self, new_database_name: str) -> None:
        self.conn.select_db(new_database_name)

    def convert_list_to_sql_string(self, data: list) -> str:
        converted_to_sql_data = [f"'{value}'"
                                 if isinstance(value, str) and value.upper() != "DEFAULT" and value.upper() != "NULL"
                                 else str(value)
                                 for value in data]
        string_values = ",".join(converted_to_sql_data)
        return string_values

    def convert_list_of_tuples_to_sql_value_string(self, data: list, separator: str) -> str:
        converted_to_sql_data = []
        for value in data:
            v0 = f"{value[0]}" if isinstance(value[0], str) and value[0].upper() != "DEFAULT" \
                                    and value[0].upper() != "NULL" else str(value[0])
            v1 = f"'{value[1]}'" if isinstance(value[1], str) and value[1].upper() != "DEFAULT" \
                                    and value[1].upper() != "NULL" else str(value[1])
            converted_to_sql_data.append(f"{v0} = {v1}")
        string_values = f" {separator} ".join(converted_to_sql_data)
        return string_values

    def insert_data(self, table_to_insert: str, data: list) -> bool:
        if not self.conn_and_cursor_exist():
            raise Exception("Connetion or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")
        if not isinstance(data, list):
            raise TypeError("Data is not a list!")

        values_string = self.convert_list_to_sql_string(data)
        sql = f"""INSERT INTO {table_to_insert} VALUES ({values_string})"""

        try:
            affected_rows = self.cursor.execute(sql)
            if affected_rows > 0:
                return True
        except:
            return False

        return False

    def delete_data(self, table_to_delete: str, where_data: list) -> bool:
        if not self.conn_and_cursor_exist():
            raise Exception("Connetion or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")
        if not isinstance(where_data, tuple):
            raise TypeError("Data is not a tuple!")

        sql = f"""DELETE FROM {table_to_delete} WHERE {where_data[0]} = {where_data[1]}"""
        try:
            affected_rows = self.cursor.execute(sql)
            if affected_rows > 0:
                return True
        except:
            return False

        return False

    def select_data(self, table_to_select: str, where_data: list) -> list:
        if not self.conn_and_cursor_exist():
            raise Exception("Connetion or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")
        if not isinstance(where_data, list):
            raise TypeError("Data is not a list!")

        wheres_string = self.convert_list_of_tuples_to_sql_value_string(where_data, 'and')
        if len(where_data) == 0:
            sql = f"""SELECT * FROM {table_to_select}"""
        else:
            sql = f"""SELECT * FROM {table_to_select} WHERE {wheres_string}"""

        query_result = []
        try:
            self.cursor.execute(sql)
            query_result.append(self.cursor.fetchall())
            return query_result
        except:
            return query_result

    def update_data(self, table_to_update: str, new_data: list, where_data: list) -> bool:
        if not self.conn_and_cursor_exist():
            raise Exception("Connetion or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")
        if not isinstance(new_data, list):
            raise TypeError("New Data is not a list!")
        if not isinstance(where_data, list):
            raise TypeError("Where Data is not a list!")

        values_string = self.convert_list_of_tuples_to_sql_value_string(new_data, ',')

        wheres_string = self.convert_list_of_tuples_to_sql_value_string(where_data, 'and')

        sql = f"""UPDATE {table_to_update} SET {values_string} WHERE {wheres_string}"""
        #print(sql)
        try:
            affected_rows = self.cursor.execute(sql)
            if affected_rows > 0:
                return True
        except:
            return False

        return False

# db = DataBase()
# db.create_connection_and_cursor("llama")
# tabela = "aqui"
# list_where = []
# print(db.select_data(tabela, list_where))

# db = DataBase()
# db.create_connection_and_cursor("llama")
# tabela = "aqui"
# delete_info = ('id', '19')
# print(db.delete_data(tabela, delete_info))

# db = DataBase()
# db.create_connection_and_cursor("llama")
# tabela = "aqui"

# db = DataBase()
# db.create_connection_and_cursor("llama")
# data_value = [(1,2),('A', 'B')]
# print(db.convert_list_of_tuples_to_sql_value_string(data_value, ','))
#
# data_where = [('1','2'), ('A', 'B')]
# print(db.convert_list_of_tuples_to_sql_value_string(data_where, 'and'))
#
# db.update_data('aqui', [('nome', 'Orlando')], [('id', 18), ('nome', 'orlando')])

#