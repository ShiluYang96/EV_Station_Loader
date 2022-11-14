import pymysql
from dbutils.pooled_db import PooledDB
from typing import Dict


class DBHelper(object):
    """
    Mysql Database helper.
    """
    def __init__(self):
        """
        Initialize a DBHelper object.
        """
        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=50,
            mincached=2,
            maxcached=3,
            blocking=True,
            setsession=[],
            ping=0,
            host='127.0.0.1',
            port=3306,
            user='root',
            password='root123',
            database='ev_db',
            charset='utf8'
        )

    def get_conn_cursor(self):
        """
        Connect with the DB.
        :return: conn, cursor.
        """
        conn = self.pool.connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        # cursor = conn.cursor()
        return conn, cursor

    def close_conn_cursor(self, *args) -> None:
        """
        Close the connection.
        :return: None.
        """
        for item in args:
            item.close()

    def exec(self, sql, **kwargs) -> None:
        """
        Execute sql command.
        :param sql: str sql command.
        :return: None.
        """
        conn, cursor = self.get_conn_cursor()

        cursor.execute(sql, kwargs)
        conn.commit()

        self.close_conn_cursor(conn, cursor)

    def fetch_one(self, sql, **kwargs) -> Dict[str, set]:
        """
        Execute the sql command and fetch the first line.
        :param sql: str sql command.
        :return: dict result of fetch.
        """
        conn, cursor = self.get_conn_cursor()

        cursor.execute(sql, kwargs)
        result = cursor.fetchone()

        self.close_conn_cursor(conn, cursor)
        return result

    def fetch_all(self, sql, **kwargs) -> Dict[str, set]:
        """
        Execute the sql command and fetch all lines.
        :param sql: str sql command.
        :return: dict result of fetch.
        """
        conn, cursor = self.get_conn_cursor()

        cursor.execute(sql, kwargs)
        result = cursor.fetchall()

        self.close_conn_cursor(conn, cursor)
        return result

    def insert(self, table, keys, vals) -> None:
        """
        Insert values in table.
        :param table: str name of table.
        :param keys: List keys of table.
        :param vals: List values to insert.
        :return: None.
        """
        conn, cursor = self.get_conn_cursor()

        query = "INSERT INTO %s " % table
        query += "(" + ",".join(["`%s`"] * len(keys)) %  tuple (keys) + ") VALUES (" + ",".join(["%s"]*len(keys)) + ")"

        cursor.executemany(query, vals)
        conn.commit()

        self.close_conn_cursor(conn, cursor)



# db = DBHelper()
# db.get_conn_cursor()
# v1 = db.fetch_one("select * from station where sid=%(nid)s", nid=3)

# my_kwargs = {"sid":2, "country_code":"some", "city":"stuttgart", "state": "badenwüttemberg", "utilization":1}
# keys = ("sid", "country_code", "city", "state", "utilization")
# values = [(1, 'de', '', 'badenwesds', 1), (2, 'de', 'stuttgart', 'badenwesds', 1)]
# db.insert("station", keys, values)