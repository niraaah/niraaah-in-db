import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from database.connectionManager import DatabaseManager
from services.jobProcessor import JobDataProcessor
from utils.logConfig import setupLogger

logger = setupLogger()

def processDataFile(filename: str):
    dbManager = None
    try:
        dbManager = DatabaseManager()
        
        # 테이블 초기화
        cursor = dbManager.dbCursor
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_categories (
                category_id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_postings (
                posting_id INT PRIMARY KEY AUTO_INCREMENT,
                company_id INT NOT NULL,
                title VARCHAR(200) NOT NULL,
                experience_level VARCHAR(50),
                education_level VARCHAR(50),
                employment_type VARCHAR(50),
                location_id INT,
                deadline_date DATE,
                job_link VARCHAR(500),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_id) REFERENCES companies(company_id),
                FOREIGN KEY (location_id) REFERENCES locations(location_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_posting_categories (
                posting_id INT,
                category_id INT,
                PRIMARY KEY (posting_id, category_id),
                FOREIGN KEY (posting_id) REFERENCES job_postings(posting_id),
                FOREIGN KEY (category_id) REFERENCES job_categories(category_id)
            )
        """)

        # 기본 카테고리 데이터 삽입
        categories = ['신입', '경력', '신입·경력', '경력무관', '인턴', '전문연구요원']
        for category in categories:
            cursor.execute("""
                INSERT IGNORE INTO job_categories (name) 
                VALUES (%s)
            """, (category,))
        
        dbManager.connection.commit()
        
        # 데이터 처리
        processor = JobDataProcessor(dbManager)
        dataFrame = pd.read_csv(filename)
        successCount = 0
        skipCount = 0
        
        for _, rowData in dataFrame.iterrows():
            try:
                result = processor.processJobEntry(rowData)
                if result:
                    successCount += 1
                    if successCount % 100 == 0:  # 진행상황 로깅
                        print(f"Processed {successCount} entries...")
                else:
                    skipCount += 1
                
            except Exception as e:
                print(f"Error processing row: {str(e)}")
                continue
        
        print(f"Processing completed. Inserted: {successCount}, Skipped: {skipCount}")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        raise
    finally:
        if dbManager:
            dbManager.closeConnection()

if __name__ == "__main__":
    try:
        csv_file = "recruitment_data.csv"  # CSV 파일 경로
        processDataFile(csv_file)
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
