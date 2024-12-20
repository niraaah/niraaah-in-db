import mysql.connector
from mysql.connector import Error
from typing import Dict, Optional

class DatabaseManager:
    def __init__(self):
        try:
            self.connection = mysql.connector.connect(
                host="113.198.66.75",
                port=10108,
                user="admin",
                password="qwer1234",
                database="wsd3",
                pool_name="mypool",
                pool_size=32,
                pool_reset_session=True,
                consume_results=True
            )
            if not self.connection.is_connected():
                raise Error("Failed to connect to database")
                
            self.dbCursor = self.connection.cursor(dictionary=True, buffered=True)
            self._initializeDatabase()
            
        except Error as e:
            print(f"Error: {e}")
            raise

    def getCursor(self):
        """새로운 커서를 반환합니다."""
        return self.connection.cursor(dictionary=True, buffered=True)

    def execute(self, query, params=None):
        """쿼리를 실행하고 결과를 반환합니다."""
        cursor = self.getCursor()
        try:
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()

    def executeOne(self, query, params=None):
        """쿼리를 실행하고 단일 결과를 반환합니다."""
        cursor = self.getCursor()
        try:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result
        finally:
            cursor.close()

    def executeInsert(self, query, params=None):
        """INSERT 쿼리를 실행하고 마지막 ID를 반환합니다."""
        cursor = self.getCursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
            return cursor.lastrowid
        finally:
            cursor.close()

    def _initializeDatabase(self):
        try:
            # users 테이블
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INT PRIMARY KEY AUTO_INCREMENT,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    password VARCHAR(200) NOT NULL,
                    name VARCHAR(50) NOT NULL,
                    phone VARCHAR(20),
                    birth_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)

            # companies 테이블
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    company_id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100) NOT NULL UNIQUE
                )
            """)

            # locations 테이블
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS locations (
                    location_id INT PRIMARY KEY AUTO_INCREMENT,
                    city VARCHAR(50) NOT NULL,
                    district VARCHAR(50) NOT NULL,
                    UNIQUE KEY unique_location (city, district)
                )
            """)

            # job_postings 테이블
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS job_postings (
                    posting_id INT PRIMARY KEY AUTO_INCREMENT,
                    company_id INT NOT NULL,
                    title VARCHAR(200) NOT NULL,
                    job_link TEXT NOT NULL,
                    experience_level VARCHAR(100),
                    education_level VARCHAR(100),
                    employment_type VARCHAR(100),
                    salary_range VARCHAR(200),
                    location_id INT,
                    deadline_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (company_id) REFERENCES companies(company_id),
                    FOREIGN KEY (location_id) REFERENCES locations(location_id)
                )
            """)

            # tech_stacks 테이블
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS tech_stacks (
                    stack_id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100) NOT NULL UNIQUE
                )
            """)

            # job_tech_stacks 테이블
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS job_tech_stacks (
                    posting_id INT,
                    stack_id INT,
                    PRIMARY KEY (posting_id, stack_id),
                    FOREIGN KEY (posting_id) REFERENCES job_postings(posting_id),
                    FOREIGN KEY (stack_id) REFERENCES tech_stacks(stack_id)
                )
            """)

            self.connection.commit()
        except Error as e:
            print(f"Error initializing database: {e}")
            self.connection.rollback()
            raise

    def _loadTechnologyData(self) -> Dict[str, int]:
        try:
            self.dbCursor.execute("SELECT stack_id, name FROM tech_stacks")
            return {row['name'].lower(): row['stack_id'] for row in self.dbCursor.fetchall()}
        except Error as e:
            print(f"Error loading technology data: {e}")
            raise

    def _loadCategoryData(self) -> Dict[str, int]:
        try:
            self.dbCursor.execute("SELECT location_id, CONCAT(city, ' ', district) as full_location FROM locations")
            return {row['full_location']: row['location_id'] for row in self.dbCursor.fetchall()}
        except Error as e:
            print(f"Error loading category data: {e}")
            raise

    def closeConnection(self):
        if self.dbCursor:
            self.dbCursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()