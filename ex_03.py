import sqlite3

def create_connection(db_file):
   """ create a database connection to the SQLite database
       specified by db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except sqlite3.Error as e:
       print(e)
   return conn

def add_project(conn, project):
   """
   Create a new project into the projects table
   :param conn:
   :param project:
   :return: project id
   """
   sql = '''INSERT INTO projects(station, latitude, longitude, elevation, name, country, state)
             VALUES(?,?,?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, project)
   conn.commit()
   return cur.lastrowid

def add_task(conn, task):
   """
   Create a new task into the tasks table
   :param conn:
   :param task:
   :return: task id
   """
   sql = '''INSERT INTO tasks(projekt_id, station, date, precip, tobs)
             VALUES(?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, task)
   conn.commit()
   return cur.lastrowid

if __name__ == "__main__":
   project = ("")

   conn = create_connection("database.db")
   pr_id = add_project(conn, project)

   task = (
       pr_id,
    ""
   )

   task_id = add_task(conn, task)

   print(pr_id, task_id)
   conn.commit()