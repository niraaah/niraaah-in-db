import mysql.connector
from mysql.connector import Error
from typing import Dict, Optional

class DatabaseManager:
    def __init__(self):
        try:
            self.connection = mysql.connector.connect(
                host="113.198.66.75",  # 실제 DB 서버 주소
                port=10108,            # 실제 포트
                user="admin",
                password="qwer1234",
                database="wsd3",
                pool_name="mypool",
                pool_size=32,
                pool_reset_session=True
            )
            if not self.connection.is_connected():
                raise Error("Failed to connect to database")
                
            self.dbCursor = self.connection.cursor(dictionary=True)
            self._initializeDatabase()
            self.techCache = self._loadTechnologyData()
            self.categoryCache = self._loadCategoryData()
        except Error as e:
            print(f"Error: {e}")
            raise

    def _initializeDatabase(self):
        try:
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