import mysql.connector
from mysql.connector import Error
from typing import Dict, Optional

class DatabaseManager:
    def __init__(self):
        try:
            self.connection = mysql.connector.connect(
                host="localhost",
                port=8080,
                user="admin",
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

    def _initializeCategories(self):
        try:
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS job_categories (
                    category_id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100) NOT NULL UNIQUE
                )
            """)
            self.connection.commit()
        except Error as e:
            raise

    def _initializeTechnologies(self):
        try:
            # users 테이블 추가
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INT PRIMARY KEY AUTO_INCREMENT,
                    username VARCHAR(100) NOT NULL UNIQUE,
                    password VARCHAR(255) NOT NULL,
                    email VARCHAR(100) UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # companies 테이블
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    company_id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100) NOT NULL UNIQUE
                )
            """)

            # tech_stacks 테이블
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS tech_stacks (
                    stack_id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100) NOT NULL UNIQUE
                )
            """)
            
            # job_postings 테이블 - 컬럼 추가
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS job_postings (
                    posting_id INT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200) NOT NULL,
                    company_id INT,
                    experience_level VARCHAR(100),
                    education_level VARCHAR(100),
                    employment_type VARCHAR(100),
                    salary_info VARCHAR(200),
                    location_id INT,
                    deadline_date VARCHAR(100),
                    job_link TEXT,
                    FOREIGN KEY (company_id) REFERENCES companies(company_id)
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
            raise

    def closeConnection(self):
        if self.dbCursor:
            self.dbCursor.close()
        if self.connection:
            self.connection.close()