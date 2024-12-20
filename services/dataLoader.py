import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from database.connectionManager import DatabaseManager
from services.jobProcessor import JobDataProcessor
from utils.logConfig import setupLogger

logger = setupLogger()

def processDataFile(csv_file: str):
    try:
        # UTF-8 인코딩으로 CSV 파일 읽기
        df = pd.read_csv(csv_file, encoding='utf-8')
        
        dbManager = DatabaseManager()
        processor = JobDataProcessor(dbManager)
        
        # 테이블 생성
        cursor = dbManager.dbCursor
        createTables(cursor)
        dbManager.connection.commit()
        
        for _, rowData in df.iterrows():
            result = processor.processJobEntry(rowData)
            if not result:
                logger.error(f"Failed to process job entry: {rowData['제목']}")
                
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
    finally:
        if 'dbManager' in locals():
            dbManager.close()

def createTables(cursor):
    # 회사 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            company_id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            logo_url VARCHAR(500),
            website_url VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 직무 카테고리 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_categories (
            category_id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 기술 스택 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tech_stacks (
            stack_id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(50) UNIQUE NOT NULL,
            category VARCHAR(50) DEFAULT 'Other'
        )
    """)

    # 위치 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            location_id INT PRIMARY KEY AUTO_INCREMENT,
            city VARCHAR(100) NOT NULL,
            district VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY city_district (city, district)
        )
    """)

    # 채용 공고 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_postings (
            posting_id INT PRIMARY KEY AUTO_INCREMENT,
            company_id INT NOT NULL,
            title VARCHAR(200) NOT NULL,
            experience_level VARCHAR(50),
            education_level VARCHAR(50),
            employment_type VARCHAR(50),
            salary_info VARCHAR(100),
            location_id INT,
            deadline_date DATE,
            job_link VARCHAR(500),
            status VARCHAR(20) DEFAULT 'active',
            view_count INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (company_id) REFERENCES companies(company_id),
            FOREIGN KEY (location_id) REFERENCES locations(location_id)
        )
    """)

    # 채용 공고-기술 스택 연결 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_tech_stacks (
            posting_id INT,
            stack_id INT,
            PRIMARY KEY (posting_id, stack_id),
            FOREIGN KEY (posting_id) REFERENCES job_postings(posting_id) ON DELETE CASCADE,
            FOREIGN KEY (stack_id) REFERENCES tech_stacks(stack_id) ON DELETE CASCADE
        )
    """)

    # 채용 공고-직무 카테고리 연결 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posting_categories (
            posting_id INT,
            category_id INT,
            PRIMARY KEY (posting_id, category_id),
            FOREIGN KEY (posting_id) REFERENCES job_postings(posting_id) ON DELETE CASCADE,
            FOREIGN KEY (category_id) REFERENCES job_categories(category_id) ON DELETE CASCADE
        )
    """)

if __name__ == "__main__":
    try:
        csv_file = "recruitment_data.csv"  # CSV 파일 경로
        processDataFile(csv_file)
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
