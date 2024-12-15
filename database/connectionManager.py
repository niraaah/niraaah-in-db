import mysql.connector
from mysql.connector import Error
from typing import Dict, Optional

class DatabaseManager:
    def __init__(self):
        try:
            self.connection = mysql.connector.connect(
                host="localhost",
                port=3000,
                user="root",
                password="qwer1234",
                database="wsd3"
            )
            self.dbCursor = self.connection.cursor(dictionary=True)
            self._initializeCategories()
            self._initializeTechnologies()
            self.techCache = self._loadTechnologyData()
            self.categoryCache = self._loadCategoryData()
        except Error as e:
            raise

    def _loadTechnologyData(self) -> Dict[str, int]:
        try:
            self.dbCursor.execute("SELECT stack_id, name FROM tech_stacks")
            return {row['name'].lower(): row['stack_id'] for row in self.dbCursor.fetchall()}
        except Error:
            return {}

    def _loadCategoryData(self) -> Dict[str, int]:
        try:
            self.dbCursor.execute("SELECT category_id, name FROM job_categories")
            return {row['name']: row['category_id'] for row in self.dbCursor.fetchall()}
        except Error:
            return {}

    def closeConnection(self):
        if self.dbCursor:
            self.dbCursor.close()
        if self.connection:
            self.connection.close()