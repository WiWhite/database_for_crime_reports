import psycopg2
import csv
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_reader(filename):
    with open(filename, 'r') as f:
        next(f)
        reader = csv.reader(f)
        yield reader


def create_dict_for_table_types(
        reader,
        table_name,
        name_schema,
        name_database,
        username1,
        username2,
        group_name1,
        group_name2
):
    len_description = []
    len_day_of_the_week = []
    len_lat = []
    len_long = []

    for file in reader:
        for row in file:
            len_description.append(len(row[2]))
            len_day_of_the_week.append(len(row[4]))
            len_lat.append(len(row[-2]))
            len_long.append(len(row[-1]))

    dict_for_create_table = {
        'max_len_description': max(len_description) + 20,
        'max_len_day_of_the_week': max(len_day_of_the_week),
        'max_len_lat': max(len_lat),
        'max_len_long': max(len_long),
        'scale_numeric_lat': max(len_lat) - 2,
        'scale_numeric_long': max(len_long) - 2,
        'table_name': table_name,
        'name_schema': name_schema,
        'name_database': name_database,
        'username1': username1,
        'username2': username2,
        'group_name1': group_name1,
        'group_name2': group_name2,
        'psql_admin': os.environ['psql_admin'],
        'db_name_psql': os.environ['db_name_psql'],
        'psql_passwd': os.environ['psql_passwd'],
    }

    return dict_for_create_table


def create_schema(job_dict):
    conn = psycopg2.connect(
        dbname=job_dict['name_database'],
        user=job_dict['psql_admin'],
        password=job_dict['psql_passwd']
    )
    cur = conn.cursor()
    cur.execute(
        'CREATE SCHEMA IF NOT EXISTS {}'.format(job_dict['name_schema'])
    )
    conn.commit()
    conn.close()


def create_db(job_dict):
    conn = psycopg2.connect(
        dbname=job_dict['db_name_psql'],
        user=job_dict['psql_admin'],
        password=job_dict['psql_passwd']
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute('CREATE DATABASE {}'.format(job_dict['name_database']))
    conn.commit()
    conn.close()


def create_table(job_dict):
    conn = psycopg2.connect(
        dbname=job_dict['name_database'],
        user=job_dict['psql_admin'],
        password=job_dict['psql_passwd']
    )
    cur = conn.cursor()
    tuple_for_create_table = (
        job_dict['table_name'],
        job_dict['max_len_description'],
        job_dict['max_len_day_of_the_week'],
        job_dict['max_len_lat'],
        job_dict['scale_numeric_lat'],
        job_dict['max_len_long'],
        job_dict['scale_numeric_long'],

    )
    cur.execute(
        """CREATE TABLE {}(
                incident_number INT PRIMARY KEY, 
                offense_code INT, 
                description VARCHAR({}), 
                date DATE, 
                day_of_the_week	VARCHAR({}), 
                lat NUMERIC({}, {}), 
                long NUMERIC({}, {}))""".format(*tuple_for_create_table)
    )
    conn.commit()
    conn.close()


def insert_data_to_table(reader, cursor, name_table, separator):
    cursor.copy_from(reader, name_table, separator)


def create_group_and_privilege(cursor, table_name, name_group):
    cursor.execute("""CREATE GROUP %s NOLOGIN;""", (name_group,))
    cursor.execute("""REVOKE ALL ON %s FROM %s;""", (table_name, name_group,))
    if name_group == 'readonly':
        cursor.execute("""GRANT SELECT ON %s TO %s""",
                       (table_name, name_group,))
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


def main():
    name_file = 'boston.csv'
    name_table = 'boston_crimes'
    name_schema = 'crimes'
    name_db = 'crimes_db'
    username1 = 'analytic1'
    username2 = 'data_scientist1'
    group_name1 = 'readonly'
    group_name2 = 'readwrite'
    reader_generator = create_reader(name_file)
    dict_for_job = create_dict_for_table_types(
        reader_generator,
        name_table,
        name_schema,
        name_db,
        username1,
        username2,
        group_name1,
        group_name2
    )

    create_db(dict_for_job)
    create_schema(dict_for_job)
    create_table(dict_for_job)


if __name__ == '__main__':
    main()
