import sqlite3
import csv

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
  # Connect to the database
   conn = create_connection("database.db")
   
   # Insert data from clean_stations.csv into the database
   with open('clean_stations.csv', 'r') as file:
       reader = csv.reader(file)
       next(reader)  # Skip header
       for row in reader:
           project = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])
           add_project(conn, project)
   pr_id = add_project(conn, project)       

   # Insert data from clean_measure.csv into the database
   with open('clean_measure.csv', 'r') as file:
       reader = csv.reader(file)
       next(reader)  # Skip header
       for row in reader:
           task = (pr_id, row[0], row[1], row[2], row[3])
           add_task(conn, task)
   
  
   # Commit changes and close connection
   conn.commit()
   


   