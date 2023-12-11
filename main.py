import json
from typing import List, Dict, Any
import pyodbc
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
from config import server, database
import logging


class DataLoader:
    def __init__(self, connection_string: str):
        self.connection = pyodbc.connect(connection_string)
        self.cursor = self.connection.cursor()

        logging.info("The database connection was opened.")

    def create_database(self) -> None:
        try:
            self.cursor.execute("""
                IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'Dormitory')
                BEGIN
                    CREATE DATABASE Dormitory;
                END;
            """)

            logging.info("The database has been successfully created.")

        except Exception as e:
            logging.error(f"An error occurred while creating database: {e}", exc_info=True)

    def create_tables(self) -> None:
        try:
            self.cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Rooms')
                BEGIN
                    CREATE TABLE Rooms (
                        RoomID INT PRIMARY KEY,
                        RoomName NVARCHAR(9)
                    );
                END;
            """)

            self.cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Rooms')
                BEGIN
                    CREATE TABLE Students (
                        Birthday DATETIMEOFFSET,
                        StudentID INT PRIMARY KEY,
                        Name NVARCHAR(50),
                        RoomID INT,
                        Sex NVARCHAR(1),
                        FOREIGN KEY (RoomID) REFERENCES Rooms(RoomID)
                    );
                END;
            """)

            self.connection.commit()
            logging.info("The tables have been successfully created.")

        except Exception as e:
            logging.error(f"An error occurred while creating tables: {e}", exc_info=True)

    def load_data(self, students: str, rooms: str) -> None:
        """Reads data from JSON files and loads into the database."""
        try:
            with open(rooms, 'r') as file:
                rooms_data = json.load(file)

            for room in rooms_data:
                room_exists_query = """
                    SELECT 1 FROM Rooms WHERE RoomID = ?;
                """
                self.cursor.execute(room_exists_query, room["id"])
                existing_room = self.cursor.fetchone()

                if not existing_room:
                    self.cursor.execute("""
                        INSERT INTO Rooms (RoomID, RoomName)
                        VALUES (?, ?)
                    """, room["id"], room["name"])

            with open(students, 'r') as file:
                students_data = json.load(file)

            for student in students_data:
                student_exists_query = """
                    SELECT 1 FROM Students WHERE StudentID = ?;
                """
                self.cursor.execute(student_exists_query, student["id"])
                existing_student = self.cursor.fetchone()

                if not existing_student:
                    self.cursor.execute("""
                        INSERT INTO Students (Birthday, StudentID, Name, RoomID, Sex)
                        VALUES (?, ?, ?, ?, ?)
                    """, student["birthday"], student["id"], student["name"], student["room"], student["sex"])

            self.connection.commit()
            logging.info("Data has been successfully loaded.")

        except Exception as e:
            logging.error(f"An error occurred while loading data: {e}", exc_info=True)

    def query_rooms_and_students_count(self) -> List[Dict[str, int]]:
        """Query: List of rooms and number of students in each of them"""
        try:
            query = """
                SELECT r.RoomID, COUNT(s.StudentID) as StudentsCount
                FROM Rooms r
                LEFT JOIN Students s ON r.RoomID = s.RoomID
                GROUP BY r.RoomID
            """

            self.cursor.execute(query)
            query_result = self.cursor.fetchall()

            rooms_and_students_count_list = [dict(RoomID=room_id, StudentsCount=students_count)
                                             for room_id, students_count in query_result]

            logging.info("""The request to get a list of rooms and the number of students in each room was completed 
            successfully.""")

            return rooms_and_students_count_list

        except Exception as e:
            logging.error(f"Error executing the request: {e}", exc_info=True)
            return []

    def query_min_avg_age_rooms(self, limit: int = 5) -> List[Dict[str, int]]:
        """"Request: 5 rooms with the smallest average age of students"""
        try:
            query = """
                SELECT TOP({}) r.RoomID, AVG(DATEDIFF(YEAR, s.Birthday, GETDATE())) as AvgAge
                FROM Rooms r
                JOIN Students s ON r.RoomID = s.RoomID
                GROUP BY r.RoomID
                ORDER BY AvgAge
            """.format(limit)

            self.cursor.execute(query)
            query_result = self.cursor.fetchall()

            query_min_avg_age_rooms_list: List[Dict[str, int]] = [
                dict(RoomID=room_id, AvgAge=avg_age)
                for room_id, avg_age in query_result
            ]

            logging.info("""The request to get a list of 5 rooms with the smallest average age of students was completed 
            successfully.""")

            return query_min_avg_age_rooms_list

        except Exception as e:
            logging.error(f"Error executing the request: {e}", exc_info=True)
            return []

    def query_max_age_difference_rooms(self, limit: int = 5) -> List[Dict[str, int]]:
        """"Request: 5 rooms with the largest age difference among students"""
        try:
            query = """
                SELECT TOP({}) r.RoomID, 
                MAX(DATEDIFF(YEAR, s.Birthday, GETDATE())) - MIN(DATEDIFF(YEAR, s.Birthday, GETDATE())) as AgeDifference
                FROM Rooms r
                JOIN Students s ON r.RoomID = s.RoomID
                GROUP BY r.RoomID
                ORDER BY AgeDifference DESC
            """.format(limit)

            self.cursor.execute(query)
            query_result = self.cursor.fetchall()

            query_max_age_difference_rooms_list: List[Dict[str, int]] = [
                dict(RoomID=room_id, AgeDifference=age_difference)
                for room_id, age_difference in query_result
            ]

            logging.info("""The request to get a list of 5 rooms with the largest age difference among the students was 
            completed successfully.""")

            return query_max_age_difference_rooms_list

        except Exception as e:
            logging.error(f"Error executing the request: {e}", exc_info=True)
            return []

    def query_gender_mismatch_rooms(self) -> List[Dict[str, int]]:
        """Query: List of rooms where mixed-sex students live"""
        try:
            query = """
                SELECT r.RoomID
                FROM Rooms r
                JOIN Students s ON r.RoomID = s.RoomID
                GROUP BY r.RoomID
                HAVING COUNT(DISTINCT s.Sex) > 1
            """

            self.cursor.execute(query)
            query_result = self.cursor.fetchall()

            query_gender_mismatch_rooms_list: List[Dict[str, int]] = [
                dict(RoomID=row[0]) for row in query_result
            ]

            logging.info("""The request to get a list of 5 rooms where mixed-sex students live was completed 
            successfully.""")

            return query_gender_mismatch_rooms_list

        except Exception as e:
            logging.error(f"Error executing the request: {e}", exc_info=True)
            return []

    def optimize_queries(self) -> None:
        """Optimizing Queries Using Indexes"""
        try:
            # Index on the RoomID field in the Students table
            index_students_room_id_query = """
                IF NOT EXISTS (
                    SELECT * FROM sys.indexes WHERE name = 'idx_Students_RoomID' AND object_id = OBJECT_ID('Students')
                )
                BEGIN
                    CREATE INDEX idx_Students_RoomID ON Students(RoomID);
                END;
            """
            self.cursor.execute(index_students_room_id_query)

            # Index on the RoomID and Birthday fields in the Students table
            index_students_room_id_birthday_query = """
                IF NOT EXISTS (
                    SELECT * FROM sys.indexes WHERE name = 'idx_Students_RoomID_Birthday' 
                    AND object_id = OBJECT_ID('Students')
                )
                BEGIN
                    CREATE INDEX idx_Students_RoomID_Birthday ON Students(RoomID, Birthday);
                END;
            """
            self.cursor.execute(index_students_room_id_birthday_query)

            # Index on the RoomID, Birthday, and Sex fields in the Students table
            index_students_room_id_birthday_sex_query = """
                IF NOT EXISTS (
                    SELECT * FROM sys.indexes 
                    WHERE name = 'idx_Students_RoomID_Birthday_Sex' AND object_id = OBJECT_ID('Students')
                )
                BEGIN
                    CREATE INDEX idx_Students_RoomID_Birthday_Sex ON Students(RoomID, Birthday, Sex);
                END;
            """
            self.cursor.execute(index_students_room_id_birthday_sex_query)

            self.connection.commit()

            logging.info("Index creation completed successfully.")

        except Exception as e:
            logging.error(f"An error occurred while creating indexes: {e}", exc_info=True)

    def close_connection(self) -> None:
        """Closes the database connection if it is open."""
        if self.connection:
            self.connection.close()

        logging.info("The database connection was closed.")


class DocumentWriter:
    @staticmethod
    def export_result(export_result: List[Dict[str, Any]]) -> None:
        try:
            format_type = input("Enter the file format to save (xml or json): ")

            if format_type.lower() != 'xml' and format_type.lower() != 'json':
                print("Unsupported format. Only XML and JSON are supported.")

            if format_type.lower() == 'json':
                json_result = json.dumps(export_result, indent=2)

                with open('result.json', 'w') as json_file:
                    json_file.write(json_result)

            if format_type.lower() == 'xml':
                # Converting the result to XML format
                root_element_name = 'Records'
                item_element_name = 'Data'

                root = ET.Element(root_element_name)

                for item in export_result:
                    item_element = ET.SubElement(root, item_element_name)
                    for key, value in item.items():
                        element = ET.SubElement(item_element, key)
                        element.text = str(value)

                filename = input("Enter the file name in filename.xml format: ")

                if not filename.endswith('.xml'):
                    print("Invalid file format. Please provide a filename with .xml extension.")
                else:
                    # Convert to indented line
                    xml_string = minidom.parseString(ET.tostring(root)).toprettyxml(indent='  ')

                    with open(filename, 'w', encoding='utf-8') as xml_file:
                        xml_file.write(xml_string)

            logging.info("Writing data to the file was successful.")

        except Exception as e:
            logging.error(f"An error occurred while writing data to the file: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                            format="%(%(levelname)s %(message)s")

        connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database}'

        data_loader = DataLoader(connection_string)

        data_loader.create_database()

        data_loader.create_tables()

        students_file_route = input("Enter the full path to the students.json file: ")
        rooms_file_route = input("Enter the full path to the rooms.json file: ")

        data_loader.load_data(students_file_route, rooms_file_route)

        document_writer = DocumentWriter()

        query_rooms_and_students_count = data_loader.query_rooms_and_students_count()
        document_writer.export_result(query_rooms_and_students_count)

        query_min_avg_age_rooms = data_loader.query_min_avg_age_rooms()
        document_writer.export_result(query_min_avg_age_rooms)

        query_max_age_difference_rooms = data_loader.query_max_age_difference_rooms()
        document_writer.export_result(query_max_age_difference_rooms)

        query_gender_mismatch_rooms = data_loader.query_gender_mismatch_rooms()
        document_writer.export_result(query_gender_mismatch_rooms)

        data_loader.optimize_queries()
    finally:
        data_loader.close_connection()
