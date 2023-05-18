# importing library json to work with JSON format
# importing library psycopg2 to work with POSTGRESQL DBMS
# importing library tqdm for statistic
# importing library os for working with operating system
# importing xml.etree.ElementTree to work with XML files
# importing error exception from psycopg2
# importing dotenv to move out credentials
# importing logging for logs
import json
import psycopg2
import tqdm
import dotenv
import argparse
import os
import xml.etree.ElementTree as ET
import logging
import flake8
import black

from psycopg2 import Error
from tqdm import tqdm
from dotenv import dotenv_values
from typing import Any

# set logging output into console  (logging is more powerful and wide functioned than standard Error e:)
logging.basicConfig(level=logging.INFO)


class DatabaseConnection:
    """Making class for connection and basic DB operations"""

    def __init__(self, config: dict) -> None:
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self) -> None:
        """Making connection to Database using .env file and dotenv lib"""
        try:
            self.connection = psycopg2.connect(user=self.config["DB_USERNAME"],
                                               password=self.config["DB_PASSWORD"],
                                               host=self.config["DB_HOST"],
                                               port=self.config["DB_PORT"],
                                               database=self.config["DB_DATABASE"])
            self.cursor = self.connection.cursor()  # initializing cursor
            logging.info("-- Connected to database...")
        except Error as e:
            logging.error(f"-- Failed to connect to database: {e}")

    def execute_query(self, query: str) -> None:
        """Sending and executing query to database"""
        self.cursor.execute(query)

    def commit(self) -> None:
        """Saving result in DB after query"""
        self.connection.commit()

    def close(self) -> None:
        """Close cursor and after close connection for saving resources"""
        self.cursor.close()
        self.connection.close()
        logging.info("-- DB connection closed")


class JSONFile:
    """ Making class for reading JSON files, r-attribute means reading only"""

    @staticmethod
    def read_file(file_path: str) -> Any:
        """reading JSON file"""
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError as e:
            logging.error(f"-- Error reading JSON file {e}")  # throw error if file not found


class XMLFile:
    """ Class for saving query results from DB to XML file """

    @staticmethod
    def save_file(data: dict, filename: str) -> None:
        """ save incoming data to file (filename)"""
        try:
            # creating root XML element
            root = ET.Element("data")

            # creating elements for columns
            columns_element = ET.SubElement(root, "columns")
            # creating elements for each column in column_names
            for column in data["columns"]:
                # creating column element inside columns element
                column_element = ET.SubElement(columns_element, "column")
                # setting column element text = column name
                column_element.text = column

            # creating elements for rows
            rows_element = ET.SubElement(root, "rows")
            # creating element for each row in rows list
            for row in data["rows"]:
                # creating element row in element rows
                row_element = ET.SubElement(rows_element, "row")
                # for each pare column-value in current row
                for column, value in row.items():
                    # creating element with the name = column
                    column_element = ET.SubElement(row_element, column)
                    # setting element text = current
                    column_element.text = str(value)

            # creating object ElementTree with root element
            tree = ET.ElementTree(root)
            # writing tree data to file
            tree.write(filename)

            logging.info(f"Query result saved into {filename}")
        except Error as e:
            logging.error(f"-- Error while creating XML file: {e}")


def execute_and_save_query_json(database: Any, result: Any, filename: str) -> None:
    """ Executing and saving query to JSON file """
    try:
        # Collecting column names with cursor.description
        column_names = [desc[0] for desc in database.cursor.description]
        # Make a dictionary, zip - connecting column name + row
        rows = [dict(zip(column_names, row)) for row in result]

        data = {
            "columns": column_names,
            "rows": rows
        }
        # Creating file (w-option new file) and put there our data
        with open(filename, "w") as f:
            json.dump(data, f)
        logging.info(f"-- Query result saved into file {filename}")

    except Error as e:
        logging.error(f"json execute and save func. error: {e}")


def execute_query_and_save_xml(database: Any, result: Any, filename: str) -> None:
    """Executing and saving XML file, the same description as in XML parent class"""
    try:
        # collecting column names
        column_names = [desc[0] for desc in database.cursor.description]
        # creating dictionary for each row
        rows = [dict(zip(column_names, row)) for row in result]

        # creating root xml element
        root = ET.Element("data")

        # creating elements for columns
        columns_element = ET.SubElement(root, "columns")

        # creating elements for each column in column_names
        for column in column_names:
            # creating column element inside columns element
            column_element = ET.SubElement(columns_element, "column")
            # setting column element text = column name
            column_element.text = column

        # creating elements for rows
        rows_element = ET.SubElement(root, "rows")
        # creating element for each row in rows list
        for row in rows:
            # creating element row in element rows
            row_element = ET.SubElement(rows_element, "row")
            # for each pare column-value in current row
            for column, value in row.items():
                # creating element with the name = column
                column_element = ET.SubElement(row_element, column)
                # setting element text = current
                column_element.text = str(value)

        # creating object ElementTree with root element
        tree = ET.ElementTree(root)
        # writing tree data to file
        tree.write(filename)

        logging.info(f"Query result saved into {filename}")
    except Error as e:
        logging.error(f"-- Error while creating XML file: {e}")


def create_indexes(database: Any) -> None:
    """Creating indexes in database"""
    try:
        create_index_query = "CREATE INDEX IF NOT EXISTS idx_room_name ON public.room(\"name\")"
        database.execute_query(create_index_query)  # using cursor.execute(query)
        logging.info("-- Index idx_room_name created")

        create_index_query = "CREATE INDEX IF NOT EXISTS idx_student_birthday ON public.student(birthday)"
        database.execute_query(create_index_query)
        logging.info("-- Index idx_student_birthday created")

        create_index_query = "CREATE INDEX IF NOT EXISTS idx_student_room ON public.student(room)"
        database.execute_query(create_index_query)
        logging.info("-- Index idx_student_room created")

        database.commit()  # saving changes to DB
        logging.info("Indexes created successfully")

    except Error as e:
        logging.error(f"-- Error while creating indexes {e}")


def main(students_file_path: str, rooms_file_path: str, output_format: str) -> Any:
    """Main program logic"""
    # Load the environment variables from the .env file
    config = dotenv_values("config.env")

    # Create an instance of the DatabaseConnection class
    database = DatabaseConnection(config)
    database.connect()

    # Read JSON files
    rooms = JSONFile.read_file(rooms_file_path)
    students = JSONFile.read_file(students_file_path)

    # Insert data into the table room (In DBMS)
    try:
        for room in tqdm(rooms, desc=f"-- Inserting values into table room DB {database.config['DB_DATABASE']}"):
            insert_query = f"INSERT INTO room(id, name) VALUES ({room['id']}, '{room['name']}')"
            database.execute_query(insert_query)

        for student in tqdm(students,
                            desc=f"-- Inserting values into table student DB {database.config['DB_DATABASE']}"):
            insert_query = f"INSERT INTO student(id, birthday, name, room, sex) " \
                           f"VALUES ({student['id']}, '{student['birthday']}', '{student['name']}', " \
                           f"{student['room']}, '{student['sex']}')"
            database.execute_query(insert_query)

        database.commit()  # saving transaction result
        logging.info("-- Data inserted")
    except Error as e:
        logging.error(f"-- Error inserting data {e}")

    # Creating folder for result files IF NOT EXISTS
    results_folder = "results"
    if not os.path.exists(results_folder):
        os.makedirs(results_folder)
        logging.info("-- Folder 'results' created")

    # Executing and saving queries
    try:
        query1 = """
            SELECT public.room.id AS room_id,
            public.room."name" AS room_name,
            COUNT(public.student.id) AS students_quantity

            FROM public.room
                INNER JOIN public.student
                ON public.room.id = public.student.room 
            GROUP BY public.room.id
            ORDER BY public.room.id; 
            """
        database.execute_query(query1)  # executing query
        result = database.cursor.fetchall()  # collecting information from query

        filename1 = os.path.join(results_folder, "query1_result")  # join folder name + file name = results/query1_r.js
        if output_format == "json":  # selecting by user before launch .py
            filename1 += ".json"  # concatenate filename + .json
            execute_and_save_query_json(database, result, filename1)  # executing function for json
        elif output_format == "xml":  # selecting by user before launch
            filename1 += ".xml"  # concatenate filename + .xml
            execute_query_and_save_xml(database, result, filename1)  # executing function for xml

        query2 = """
            SELECT public.room.id AS room_id,
            public.room."name" AS room_name,
            COUNT(public.student.id) AS students_quantity,
            AVG(EXTRACT(YEAR FROM age(now(), public.student.birthday))) ::INTEGER AS average_age

            FROM public.room
                INNER JOIN public.student
                ON public.room.id = public.student.room
            GROUP BY public.room.id 
            ORDER BY average_age ASC
            LIMIT 5;
            """

        database.execute_query(query2)
        result = database.cursor.fetchall()

        filename2 = os.path.join(results_folder, "query2_result")
        if output_format == "json":
            filename2 += ".json"
            execute_and_save_query_json(database, result, filename2)
        elif output_format == "xml":
            filename2 += ".xml"
            execute_query_and_save_xml(database, result, filename2)

        query3 = """
            SELECT public.room.id AS room_id,
            public.room."name" AS room_name,
            COUNT(public.student.id) AS students_quantity,
            MAX(EXTRACT(YEAR FROM age(now(), public.student.birthday))) ::INTEGER -
            MIN(EXTRACT(YEAR FROM age(now(), public.student.birthday))) ::INTEGER AS stud_age_diff

            FROM public.room
                INNER JOIN public.student
                ON public.room.id = public.student.room
            GROUP BY public.room.id
            ORDER BY stud_age_diff DESC, students_quantity ASC
            LIMIT 5;
            """

        database.execute_query(query3)
        result = database.cursor.fetchall()

        filename3 = os.path.join(results_folder, "query3_result")
        if output_format == "json":
            filename3 += ".json"
            execute_and_save_query_json(database, result, filename3)
        elif output_format == "xml":
            filename3 += ".xml"
            execute_query_and_save_xml(database, result, filename3)

        query4 = """
            SELECT public.room.id AS room_id,
            public.room."name" AS room_name,
            STRING_AGG(public.student.sex, ', ' ORDER BY public.student.sex) AS genders_in_room

            FROM public.room
                INNER JOIN public.student
                ON public.room.id = public.student.room
            WHERE public.student.sex IN (UPPER('M'), UPPER('F'))
            GROUP BY public.room.id
            HAVING COUNT(DISTINCT public.student.sex) = 2
            ORDER BY public.room.id;
            """

        database.execute_query(query4)
        result = database.cursor.fetchall()

        filename4 = os.path.join(results_folder, "query4_result")
        if output_format == "json":
            filename4 += ".json"
            execute_and_save_query_json(database, result, filename4)
        elif output_format == "xml":
            filename4 += ".xml"
            execute_query_and_save_xml(database, result, filename4)

    except Error as e:
        logging.error(f"Error while executing queries {e}")

    finally:
        database.close()  # closing cursor and connection


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database Query and Export \
                                                 --example Python file.py source/students.json source/rooms.json json")
    parser.add_argument("students", type=str, help="Path to the students file")
    parser.add_argument("rooms", type=str, help="Path to the rooms file")
    parser.add_argument("format", choices=["json", "xml"], help="Output format (json or xml)")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    """
    level=logging.INFO: Sets the logging level to INFO, only messages with a level of INFO or higher will be logged --
    format="%(asctime)s - %(levelname)s - %(message)s - the record will include the event time information(%(asctime)s),
    the logging level (%(levelname)s), and the message itself (%(message)s)
    """

    main(args.students, args.rooms, args.format)
