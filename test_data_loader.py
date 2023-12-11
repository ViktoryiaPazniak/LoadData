import unittest
from unittest.mock import MagicMock, patch

from config import database, server
from main import DataLoader


class TestDataLoaderClass(unittest.TestCase):
    def setUp(self):
        # Create a layout for a database connection object
        self.mock_connection = MagicMock(name="connection")
        self.mock_cursor = MagicMock(name="cursor")
        self.mock_connection.cursor.return_value = self.mock_cursor

        # Create a layout for a class object
        self.mock_db = MagicMock(name="database")
        self.mock_db.connection = self.mock_connection

        # Patch pyodbc.connect so that it returns our connection layout
        self.patcher = patch(
            "pyodbc.connect", return_value=self.mock_connection
        )
        self.patcher.start()

        self.test_class = DataLoader(
            f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database}"
        )

    def tearDown(self):
        # Stop the patcher after running the tests
        self.patcher.stop()

    def test_query_rooms_and_students_count_success(self):
        query_result_data = [(1, 5), (2, 3)]
        expected_result = [
            {"RoomID": 1, "StudentsCount": 5},
            {"RoomID": 2, "StudentsCount": 3},
        ]

        # Setting return values for execute and fetchall
        self.mock_cursor.fetchall.return_value = [
            (room_id, students_count)
            for room_id, students_count in query_result_data
        ]

        # Calling the method we are testing
        result = self.test_class.query_rooms_and_students_count()

        # Checking that execute was called with the correct SQL query (ignoring indentation formatting)
        self.mock_cursor.execute.assert_called_once_with(unittest.mock.ANY)

        # Checking that the returned result matches the expected result
        self.assertEqual(result, expected_result)

    def test_query_rooms_and_students_count_exception(self):
        self.mock_cursor.execute.side_effect = Exception("Test Exception")

        result = self.test_class.query_rooms_and_students_count()

        self.assertEqual(result, [])

    def test_query_min_avg_age_rooms_success(self):
        query_result_data = [(1, 25), (2, 27), (3, 22), (4, 26), (5, 24)]
        expected_result = [
            {"RoomID": 3, "AvgAge": 22},
            {"RoomID": 5, "AvgAge": 24},
            {"RoomID": 1, "AvgAge": 25},
            {"RoomID": 4, "AvgAge": 26},
            {"RoomID": 2, "AvgAge": 27},
        ]

        self.mock_cursor.fetchall.return_value = query_result_data

        result = self.test_class.query_min_avg_age_rooms()

        self.mock_cursor.execute.assert_called_once_with(unittest.mock.ANY)

        self.assertEqual(len(result), len(expected_result))
        for elem1 in result:
            self.assertIn(elem1, expected_result)

    def test_query_min_avg_age_rooms_exception(self):
        self.mock_cursor.execute.side_effect = Exception("Test Exception")

        result = self.test_class.query_min_avg_age_rooms()

        self.assertEqual(result, [])

    def test_query_max_age_difference_rooms_success(self):
        query_result_data = [(1, 10), (2, 8), (3, 15), (4, 12), (5, 7)]
        expected_result = [
            {"RoomID": 3, "AgeDifference": 15},
            {"RoomID": 1, "AgeDifference": 10},
            {"RoomID": 4, "AgeDifference": 12},
            {"RoomID": 2, "AgeDifference": 8},
            {"RoomID": 5, "AgeDifference": 7},
        ]

        self.mock_cursor.fetchall.return_value = query_result_data

        result = self.test_class.query_max_age_difference_rooms()

        self.mock_cursor.execute.assert_called_once_with(unittest.mock.ANY)

        self.assertCountEqual(result, expected_result)

    def test_query_max_age_difference_rooms_exception(self):
        self.mock_cursor.execute.side_effect = Exception("Test Exception")

        result = self.test_class.query_max_age_difference_rooms()

        self.assertEqual(result, [])

    def test_query_gender_mismatch_rooms_success(self):
        query_result_data = [(1,), (3,), (5,)]
        expected_result = [{"RoomID": 1}, {"RoomID": 3}, {"RoomID": 5}]

        self.mock_cursor.fetchall.return_value = query_result_data

        result = self.test_class.query_gender_mismatch_rooms()

        self.mock_cursor.execute.assert_called_once_with(unittest.mock.ANY)

        self.assertCountEqual(result, expected_result)

    def test_query_gender_mismatch_rooms_exception(self):
        self.mock_cursor.execute.side_effect = Exception("Test Exception")

        result = self.test_class.query_gender_mismatch_rooms()

        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
