import sqlite3
from sqlite3 import Error
from contextlib import contextmanager

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

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

def add_employer(conn, employer):
    sql = '''INSERT INTO employer(first_name,last_name,phone_number,company)
        VALUES(?,?,?,?)'''
    cursor = conn.cursor()
    cursor.execute(sql,employer)
    conn.commit()
    return cursor.lastrowid

def add_employee(conn, employee):
    sql = """INSERT INTO employee(employer_id, first_name, last_name, phone_number, job)
        VALUES(?,?,?,?,?)"""
    cursor = conn.cursor()
    cursor.execute(sql,employee)
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
    update status, begin_date, and end date of a employee
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
        if v is None:
            qs.append(f"{k} IS NULL")
        else:
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
    create_employer_sql = """
    -- employer table
    CREATE TABLE IF NOT EXISTS employer (
      id integer PRIMARY KEY,
      first_name text NOT NULL,
      last_name text NOT NULL,
      phone_number varchar(12),
      company text NOT NULL
    );
    """
    create_employee_sql = """
    -- employee table
    CREATE TABLE IF NOT EXISTS employee (
      id integer PRIMARY KEY,
      employer_id integer NOT NULL,
      first_name text NOT NULL,
      last_name text NOT NULL,
      phone_number varchar(12),
      job text NOT NULL,
      FOREIGN KEY (employer_id) REFERENCES employer (id)
    );
    """
    conn = create_connection("kodilla_database_2.db")
    execute_sql(conn,create_employer_sql)
    execute_sql(conn,create_employee_sql)
    add_employer(conn,("Wyatt","Black","+44555444333","Aero Inc."))
    add_employer(conn,("Lana","Anderson",None,"Musica Ltd."))
    employee=(1,"Maya","Johnson","+44000111222","Architect")
    add_employee(conn,employee)
    employee=(1,"Jessica","Leone",None,"Engineer")
    add_employee(conn,employee)
    employee=(2,"Dave","Matthews","+44777333222","Manager")
    add_employee(conn,employee)
    employee=(2,"Ben","Davies",None,"Engineer")
    add_employee(conn,employee)
    update(conn,"employee",2,phone_number="+44888222111")
    delete_where(conn,"employee",phone_number=None)
    print("To są menadżerowie:")
    print(select_where(conn, "employee", job="Manager"))
    print("To są architekci:")
    print(select_where(conn, "employee", job="Architect"))