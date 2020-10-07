import psycopg2
import csv


def create_reader(filename):
    with open(filename, 'r') as f:
        next(f)
        reader = csv.reader(f)
        return reader


def create_dict_for_table_types(reader):
    len_offense_code = []
    len_description = []
    len_day_of_the_week = []
    len_lat = []
    len_long = []

    dict_for_create_table = {
        'max_len_offense_code': max(len_offense_code),
        'max_len_description': max(len_description),
        'max_len_day_of_the_week': max(len_day_of_the_week),
        'max_len_lat': max(len_lat),
        'max_len_long': max(len_long),
        'scale_numeric_lat': max(len_lat) - 2,
        'scale_numeric_long': max(len_long) - 2
    }

    for row in reader:
        len_offense_code.append(len(row[1]))
        len_description.append(len(row[2]))
        len_day_of_the_week.append(len(row[4]))
        len_lat.append(len(row[-2]))
        len_long.append(len(row[-1]))

    return dict_for_create_table


def create_connnector(dbname, user, password):
    connector = psycopg2.connect(dbname=dbname, user=user, password=password)
    return connector


def create_cursor(connector):
    cursor = connector.cursor()
    return cursor


def create_schema(cursor, name_schema):
    cursor.execute("""CREATE SCHEMA %s;""", (name_schema,))


def create_db(cursor, name_db, name_schema):
    cursor.execute("""USE %s;""", (name_schema,))
    cursor.execute("""CREATE DATABASE %s;""", (name_db,))


def create_table(cursor, dict_for_create):
    cursor.execute(
        """CREATE TABLE boston_crimes(
                incident_number INT PRIMARY KEY, 
                offense_code INT(%(max_len_offense_code)s), 
                description VARCHAR(%(max_len_description)s), 
                date DATE, 
                day_of_the_week	VARCHAR(%(max_len_day_of_the_week)s), 
                lat NUMERIC(%(max_len_lat)s, %(scale_lat)s), 
                long NUMERIC(%(max_len_long)s), %(scale_long)s);""",
        dict_for_create
    )


def insert_data_to_table(reader, cursor, name_table, separator):
    cursor.copy_from(reader, name_table, separator)


def create_group_and_privilege(cursor, table_name, name_group):
    cursor.execute("""CREATE GROUP %s NOLOGIN;""", (name_group,))
    cursor.execute("""REVOKE ALL ON %s FROM %s;""", (table_name, name_group,))
    if name_group == 'readonly':
        cursor.execute("""GRANT SELECT ON %s TO %s""", (table_name, name_group,))
    elif name_group == 'readwrite':
        cursor.execute(
            """GRANT SELECT, INSERT, UPDATE ON %s TO %s""",
            (table_name, name_group,)
        )


def create_user(cursor, user_name, password):
    cursor.execute(
        """CREATE USER %s NOSUPERUSER PASSWORD %s;""",
        (user_name, password,)
    )


def add_user_to_group(cursor, user_name, group_name):
    cursor.execute("""GRANT %s TO %s;""", (group_name, user_name,))


def commit_changes(connector):
    connector.commit()


def close_connection(connector):
    connector.close()
