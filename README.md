# Project: Importing student and room data into a database

## Description

The project provides a Python script to load student and room data into a MS SQL Server relational database. The script runs the necessary database queries, provides query optimization using indexes, and generates an SQL query to add the required indexes.

## Requirements

1. Python 3.x
2. Database (MS SQL Server)
3. File with student data (students.json)
4. File with data about rooms (rooms.json)

## Installation

1. Clone the repository:
   git clone https://github.com/ViktoryiaPazniak/LoadData.git
2. Install dependencies:
   pip install -r requirements.txt

## Usage

1. Run the script, specifying the path to the file with student data, the path to the file with room data and the output format (xml or json).
2. Follow the instructions on the screen.

## Query optimization

The project implements optimized queries using indexes. The script also provides an SQL query to add the required indexes to the database.

## Database queries

1. List of rooms and the number of students in each of them.
    python main.py --query rooms_students_count
   
3. 5 rooms with the smallest average age of students.
   python main.py --query min_avg_age_rooms --limit 5
   
5. 5 rooms with the largest difference in the age of students.
   python main.py --query max_age_difference_rooms --limit 5
   
7. List of rooms where mixed-sex students live.
   python main.py --query gender_mismatch_rooms

## Uploading the result

Query results are saved in JSON or XML format to the output.json or output.xml file.
