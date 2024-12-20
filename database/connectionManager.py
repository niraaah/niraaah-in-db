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
            # 1. 기본 테이블 (외래 키 없는 테이블)
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

            # 2. job_postings 테이블
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

            # 3. users 테이블
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INT PRIMARY KEY AUTO_INCREMENT,
                    username VARCHAR(100) NOT NULL UNIQUE,
                    password_hash VARCHAR(255) NOT NULL,
                    email VARCHAR(100) UNIQUE,
                    name VARCHAR(100),
                    phone VARCHAR(20),
                    birth_date DATE,
                    status VARCHAR(20) DEFAULT 'active',
                    last_login TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 4. 나머지 테이블들 (외래 키 있는 테이블)
            # ... (나머��� 테이블 생성 코드)
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS resumes (
                    resume_id INT PRIMARY KEY AUTO_INCREMENT,
                    user_id INT NOT NULL,
                    title VARCHAR(200) NOT NULL,
                    content LONGBLOB,
                    is_primary BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)

            # applications 테이블 추가
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS applications (
                    application_id INT PRIMARY KEY AUTO_INCREMENT,
                    user_id INT NOT NULL,
                    posting_id INT NOT NULL,
                    resume_id INT,
                    status VARCHAR(50) NOT NULL DEFAULT 'pending',
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (posting_id) REFERENCES job_postings(posting_id),
                    FOREIGN KEY (resume_id) REFERENCES resumes(resume_id)
                )
            """)

            # bookmarks 테이블 추가
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS bookmarks (
                    bookmark_id INT PRIMARY KEY AUTO_INCREMENT,
                    user_id INT NOT NULL,
                    posting_id INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (posting_id) REFERENCES job_postings(posting_id)
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

            # locations 테이블 추가
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS locations (
                    location_id INT PRIMARY KEY AUTO_INCREMENT,
                    city VARCHAR(50) NOT NULL,
                    district VARCHAR(50) NOT NULL,
                    UNIQUE KEY unique_location (city, district)
                )
            """)

            # recruitment_posts 테이블 추가
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS recruitment_posts (
                    post_id INT PRIMARY KEY AUTO_INCREMENT,
                    collected_at TIMESTAMP NOT NULL,
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

            # post_tech_stacks 테이블 추가 (Many-to-Many 관계)
            self.dbCursor.execute("""
                CREATE TABLE IF NOT EXISTS post_tech_stacks (
                    post_id INT,
                    stack_id INT,
                    PRIMARY KEY (post_id, stack_id),
                    FOREIGN KEY (post_id) REFERENCES recruitment_posts(post_id),
                    FOREIGN KEY (stack_id) REFERENCES tech_stacks(stack_id)
                )
            """)
            
            self.connection.commit()
        except Error as e:
            raise

    def insert_location(self, city: str, district: str) -> int:
        try:
            self.dbCursor.execute("""
                INSERT IGNORE INTO locations (city, district)
                VALUES (%s, %s)
            """, (city, district))
            
            self.connection.commit()
            
            self.dbCursor.execute("""
                SELECT location_id FROM locations
                WHERE city = %s AND district = %s
            """, (city, district))
            
            return self.dbCursor.fetchone()['location_id']
        except Error as e:
            raise

    def insert_company(self, company_name: str) -> int:
        try:
            self.dbCursor.execute("""
                INSERT IGNORE INTO companies (name)
                VALUES (%s)
            """, (company_name,))
            
            self.connection.commit()
            
            self.dbCursor.execute("""
                SELECT company_id FROM companies
                WHERE name = %s
            """, (company_name,))
            
            return self.dbCursor.fetchone()['company_id']
        except Error as e:
            raise

    def insert_recruitment_post(self, post_data: dict) -> int:
        try:
            # 회사 정보 삽입
            company_id = self.insert_company(post_data['company_name'])
            
            # 위치 정보 파싱 및 삽입
            city, district = post_data['location'].split(' ')
            location_id = self.insert_location(city, district)
            
            # 채용공고 정보 삽입
            self.dbCursor.execute("""
                INSERT INTO recruitment_posts (
                    collected_at, company_id, title, job_link,
                    experience_level, education_level, employment_type,
                    salary_range, location_id, deadline_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                post_data['collected_at'],
                company_id,
                post_data['title'],
                post_data['job_link'],
                post_data['experience_level'],
                post_data['education_level'],
                post_data['employment_type'],
                post_data['salary_range'],
                location_id,
                post_data['deadline_date']
            ))
            
            post_id = self.dbCursor.lastrowid
            
            # 기술 스택 정보 삽입
            tech_stacks = [stack.strip() for stack in post_data['tech_stacks'].split(',')]
            for tech in tech_stacks:
                if tech.lower() in self.techCache:
                    stack_id = self.techCache[tech.lower()]
                    self.dbCursor.execute("""
                        INSERT IGNORE INTO post_tech_stacks (post_id, stack_id)
                        VALUES (%s, %s)
                    """, (post_id, stack_id))
            
            self.connection.commit()
            return post_id
        except Error as e:
            raise

    def closeConnection(self):
        if self.dbCursor:
            self.dbCursor.close()
        if self.connection:
            self.connection.close()