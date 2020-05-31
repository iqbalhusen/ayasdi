import csv
import os
import random
import math
import datetime
import sqlite3

TOTAL_ROWS = 1000000
NULL_VALUE_ROWS = int(TOTAL_ROWS * 0.01)
SQLITE3_COLUMNS = []
CSV_FILE_PATH = 'ayasdi_assignment.csv'
DB_TABLE_NAME = 'ayasdi_assignment'


def get_words_list_from_dictionary():
    with open('words.txt', 'r') as f:
        return [line.strip() for line in f]


def get_random_date(start_date, end_date):
    days_in_between = (end_date - start_date).days
    add_days = random.choice(range(days_in_between))
    return start_date + datetime.timedelta(days=add_days)


def computer_normal_distribution(x, mean, standard_deviation):
    denominator = standard_deviation * math.sqrt(2*math.pi)
    numerator = math.exp(-0.5 * pow((x-mean) / standard_deviation, 2))
    return numerator / denominator


def generate_fieldnames():
    fieldnames = ['col1']
    sqlite3_columns = {'col1': 'INTEGER'}
    null_value_indices = {}

    for i in range(9):
        field_name = 'col%d_%d' % (i + 2, i)
        fieldnames.append(field_name)
        sqlite3_columns[field_name] = 'REAL'
        null_value_indices[field_name] = set(random.sample(range(1, TOTAL_ROWS), NULL_VALUE_ROWS))

    for i in range(11, 20):
        field_name = 'col%d' % i
        fieldnames.append(field_name)
        sqlite3_columns[field_name] = 'TEXT'
        null_value_indices[field_name] = set(random.sample(range(1, TOTAL_ROWS), NULL_VALUE_ROWS))

    fieldnames.append('col20')
    sqlite3_columns['col20'] = 'TEXT'

    global SQLITE3_COLUMNS
    SQLITE3_COLUMNS = sqlite3_columns

    return fieldnames, null_value_indices


def create_db():
    db_file = 'ayasdi_assignment.db'
    if os.path.isfile(db_file):
        os.remove(db_file)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    columns = ''

    for column_name, column_type in SQLITE3_COLUMNS.items():
        columns += '%s %s, ' % (column_name, column_type)

    columns = columns[:-2]
    c.execute('CREATE TABLE %s (%s)' % (DB_TABLE_NAME, columns))
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS col1_UNIQUE ON ayasdi_assignment (col1)')

    with open(CSV_FILE_PATH, newline='', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)

        i = 0
        for row in reader:
            i += 1
            print('Inserting in to sqlite: %d of %d' % (i, TOTAL_ROWS))
            values = ''
            for column_name, column_type in SQLITE3_COLUMNS.items():
                if column_type == 'TEXT' and row[column_name]:
                    values += "'%s', " % row[column_name]
                else:
                    values += '%s, ' % (row[column_name] if row[column_name] else 'NULL')

            values = values[:-2]
            c.execute("INSERT INTO %s VALUES (%s)" % (DB_TABLE_NAME, values))

    conn.commit()
    conn.close()


def create_csv():
    if os.path.isfile(CSV_FILE_PATH):
        os.remove(CSV_FILE_PATH)

    with open(CSV_FILE_PATH, 'w', newline='') as csv_file:
        fieldnames, null_value_indices = generate_fieldnames()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        words_list = get_words_list_from_dictionary()
        start_date = datetime.date(2014, 1, 1)
        end_date = datetime.date(2014, 12, 31)

        for i in range(TOTAL_ROWS):
            print('Working on row: %d of %d' % (i, TOTAL_ROWS))
            row_dict = {}
            for field_name in fieldnames:
                if field_name == 'col1':
                    row_dict[field_name] = i + 1
                elif '_' in field_name:
                    if i + 1 in null_value_indices[field_name]:
                        row_dict[field_name] = None
                    else:
                        mean = float(field_name.split('_')[1])
                        standard_deviation = random.uniform(0, 10)

                        # generating value of "x" randomly as no instruction provided on how to choose "x"
                        x = random.uniform(0, 10)
                        normal_distribution = computer_normal_distribution(x, mean, standard_deviation)
                        row_dict[field_name] = normal_distribution
                elif field_name != 'col20':
                    if i + 1 in null_value_indices[field_name]:
                        row_dict[field_name] = None
                    else:
                        row_dict[field_name] = random.choice(words_list)
                else:
                    row_dict[field_name] = get_random_date(start_date, end_date)
            writer.writerow(row_dict)

    create_db()


if __name__ == '__main__':
    create_csv()
