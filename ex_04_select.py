import sqlite3
from sqlite3 import Error

def create_connection(db_file):
   """ create a database connection to a SQLite database """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")
   except Error as e:
       print(e)
   return conn

def select_task_by_status(conn, status):
   """
   Query tasks by priority
   :param conn: the Connection object
   :param status:
   :return:
   """
   cur = conn.cursor()
   cur.execute("SELECT * FROM tasks WHERE status=?", (status,))

   rows = cur.fetchall()
   return rows

def select_all(conn, table):
   """
   Query all rows in the table
   :param conn: the Connection object
   :return:
   """
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows


if __name__ == "__main__":
   conn = create_connection("database.db")
   conn.execute("SELECT * FROM tasks LIMIT 5").fetchall()
   conn.close()
print(conn.execute("SELECT * FROM tasks LIMIT 5").fetchall())
   