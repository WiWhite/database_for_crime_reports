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
    dict_for_create_table = {'max_len_offense_code': max(len_offense_code),
                             'max_len_description': max(len_description),
                             'max_len_day_of_the_week': max(len_day_of_the_week),
                             'max_len_lat': max(len_lat),
                             'max_len_long': max(len_long),
                             'scale_numeric_lat': max(len_lat) - 2,
                             'scale_numeric_long': max(len_long) - 2}
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
    cursor.execute("""CREATE SCHEMA %s""", name_schema)


def create_table(cursor, dict_for_create):
    cursor.execute("""CREATE TABLE crimes_db(
                    incident_number INT PRIMARY KEY, 
                    offense_code INT(%(max_len_offense_code)s), 
                    description VARCHAR(%(max_len_description)s), 
                    date DATE, 
                    day_of_the_week	VARCHAR(%(max_len_day_of_the_week)s), 
                    lat NUMERIC(%(max_len_lat)s, %(scale_lat)s), 
                    long NUMERIC(%(max_len_long)s), %(scale_long)s);""", dict_for_create)


def insert_data_to_table(reader, cursor):
    pass
