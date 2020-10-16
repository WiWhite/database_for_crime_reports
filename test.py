import psycopg2
import csv
import os


def create_reader_yield(filename):
    with open(filename, 'r') as f:
        next(f)
        reader = csv.reader(f)
        yield reader


def create_dict_for_table_types_and_other(
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
    conn.autocommit = True
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
                incident_number SERIAL PRIMARY KEY, 
                offense_code INT, 
                description VARCHAR({}), 
                date DATE, 
                day_of_the_week	VARCHAR({}), 
                lat NUMERIC({}, {}), 
                long NUMERIC({}, {}))""".format(*tuple_for_create_table)
    )
    conn.commit()
    conn.close()


def insert_data_to_table(filename, job_dict, separator):
    conn = psycopg2.connect(
        dbname=job_dict['name_database'],
        user=job_dict['psql_admin'],
        password=job_dict['psql_passwd']
    )
    with open(filename, 'r') as f:
        next(f)
        cur = conn.cursor()
        cur.copy_from(f, job_dict['table_name'], separator)
    conn.commit()
    conn.close()


def create_group_and_privilege(job_dict, table_name, name_group):
    conn = psycopg2.connect(
        dbname=job_dict['name_database'],
        user=job_dict['psql_admin'],
        password=job_dict['psql_passwd']
    )
    cur = conn.cursor()
    cur.execute("""CREATE GROUP {} NOLOGIN;""".format(name_group))
    cur.execute("""REVOKE ALL ON {} FROM {};""".format(
        table_name, name_group)
    )
    if name_group == 'readonly':
        cur.execute("""GRANT SELECT ON {} TO {}""".format(
            table_name, name_group)
        )
    elif name_group == 'readwrite':
        cur.execute("""GRANT SELECT, INSERT, UPDATE ON {} TO {}""".format(
            table_name, name_group)
        )
    conn.commit()
    conn.close()


def create_user(job_dict, user_name, password):
    conn = psycopg2.connect(
        dbname=job_dict['name_database'],
        user=job_dict['psql_admin'],
        password=job_dict['psql_passwd']
    )
    cur = conn.cursor()
    cur.execute(
        """CREATE USER {} NOSUPERUSER PASSWORD '{}';""".format(
            user_name, password)
    )
    conn.commit()
    conn.close()


def add_user_to_group(job_dict, user_name, group_name):
    conn = psycopg2.connect(
        dbname=job_dict['name_database'],
        user=job_dict['psql_admin'],
        password=job_dict['psql_passwd']
    )
    cur = conn.cursor()
    cur.execute("""GRANT {} TO {};""".format(group_name, user_name))
    conn.commit()
    conn.close()


def main():
    name_file = 'boston.csv'
    name_table = 'boston_crimes'
    name_schema = 'crimes'
    name_db = 'crimes_db'
    username1 = 'analytic1'
    username2 = 'data_scientist1'
    group_name1 = 'readonly'
    group_name2 = 'readwrite'
    reader_generator = create_reader_yield(name_file)
    dict_for_job = create_dict_for_table_types_and_other(
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
    insert_data_to_table(name_file, dict_for_job, ',')
    create_group_and_privilege(
        dict_for_job,
        dict_for_job['table_name'],
        dict_for_job['group_name1']
    )
    create_group_and_privilege(
        dict_for_job,
        dict_for_job['table_name'],
        dict_for_job['group_name2']
    )
    create_user(dict_for_job, dict_for_job['username1'], '1234509876')
    create_user(dict_for_job, dict_for_job['username2'], '1234567890')
    add_user_to_group(
        dict_for_job,
        dict_for_job['username1'],
        dict_for_job['group_name1']
    )
    add_user_to_group(
        dict_for_job,
        dict_for_job['username2'],
        dict_for_job['group_name2']
    )


if __name__ == '__main__':
    main()
