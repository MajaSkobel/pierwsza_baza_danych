# `ex_01_conection_to_db.py`

import sqlite3
from sqlite3 import Error
from contextlib import contextmanager

def create_connection(db_file):
   """ create a database connection to a SQLite database """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return(conn)
   except Error as e:
       print(e)
   return(conn)

def create_connection_in_memory():
   """ create a database connection to a SQLite database """
   conn = None
   try:
       conn = sqlite3.connect(":memory:")
       print(f"Connected, sqlite version: {sqlite3.version}")
   except Error as e:
       print(e)
   finally:
       if conn:
           conn.close()

def execute_sql(conn, sql):
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)

def add_project(conn, project):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    sql = '''INSERT INTO projects(nazwa, start_date, end_date)
        VALUES(?,?,?)'''
    cursor = conn.cursor()
    cursor.execute(sql, project)
    conn.commit()
    return cursor.lastrowid

def add_task(conn, task):
    sql = """INSERT INTO tasks(project_id, nazwa, opis, status, start_date, end_date)
        VALUES(?,?,?,?,?,?)"""
    cursor = conn.cursor()
    cursor.execute(sql, task)
    conn.commit()
    return cursor.lastrowid

def select_all(conn, table):
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()
   return rows

def select_where(conn, table, **query):
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

def update(conn, table, id, **kwargs):
   """
   update status, begin_date, and end date of a task
   :param conn:
   :param table: table name
   :param id: row id
   :return:
   """
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)

def delete_where(conn, table, **kwargs):
   """
   Delete from table where attributes from
   :param conn:  Connection to the SQLite database
   :param table: table name
   :param kwargs: dict of attributes and values
   :return:
   """
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)

   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Deleted")

def delete_all(conn, table):
   """
   Delete all rows from table
   :param conn: Connection to the SQLite database
   :param table: table name
   :return:
   """
   sql = f'DELETE FROM {table}'
   cur = conn.cursor()
   cur.execute(sql)
   conn.commit()
   print("Deleted")

if __name__ == '__main__':
    create_projects_sql = """
   -- projects table
   CREATE TABLE IF NOT EXISTS projects (
      id integer PRIMARY KEY,
      nazwa text NOT NULL,
      start_date text,
      end_date text
   );
   """
    create_tasks_sql = """
    -- tasks table
   CREATE TABLE IF NOT EXISTS tasks (
      id integer PRIMARY KEY,
      project_id integer NOT NULL,
      nazwa VARCHAR(250) NOT NULL,
      opis TEXT,
      status VARCHAR(15) NOT NULL,
      start_date text NOT NULL,
      end_date text NOT NULL,
      FOREIGN KEY (project_id) REFERENCES projects (id)
   );
   """
    my_database = create_connection("kodilla_database.db")
    with sqlite3.connect("kodilla_database.db") as conn:
        execute_sql(conn,create_projects_sql)
        execute_sql(conn,create_tasks_sql)
        add_project(conn,("Obowiązki domowe",None,None))
        add_project(conn,("Inne obowiązki",None,"20.09.2024"))
        task=(1,"Wyrzucić śmieci",None,"not started","17.09.2024","17.09.2024")
        add_task(conn,task)
        task=(1,"Pozmywać naczynia",None,"started","17.09.2024","17.09.2024")
        add_task(conn,task)
        task=(2,"Pójść na siłownię","Trening nóg","not started","17.09.2024","18.09.2024")
        add_task(conn,task)
        task=(2,"Umówić się do lekarza","Zadzwonić do +48 000 000 000","ended","17.09.2024","19.09.2024")
        add_task(conn,task)
        update(conn,"tasks",2,status="ended")
        delete_where(conn,"tasks",status="ended")
        print("To musisz dokończyć:")
        print(select_where(conn, "tasks", status="started"))
        print("To musisz zacząć:")
        print(select_where(conn, "tasks", status="not started"))