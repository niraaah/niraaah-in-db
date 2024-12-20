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
        processor = JobDataProcessor(dbManager)
        
        # CSV 파일 읽기
        dataFrame = pd.read_csv(filename, parse_dates=['timestamp', '마감일'])
        successCount = 0
        skipCount = 0
        
        # 각 행 처리
        for _, row in dataFrame.iterrows():
            try:
                # 데이터 매핑
                jobData = {
                    'company_name': row['회사명'],
                    'title': row['제목'],
                    'link': row['링크'],
                    'experience': row['경력'],
                    'education': row['학력'],
                    'employment_type': row['고용형태'],
                    'salary': row['연봉정보'],
                    'location': row['지역'],
                    'tech_stack': row['직무분야'],
                    'deadline': row['마감일']
                }
                
                result = processor.processJobEntry(jobData)
                if result:
                    successCount += 1
                    if successCount % 100 == 0:
                        print(f"Processed {successCount} entries...")
                else:
                    skipCount += 1
                
            except Exception as e:
                logger.error(f"Error processing row: {str(e)}")
                continue
        
        print(f"Processing completed. Inserted: {successCount}, Skipped: {skipCount}")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
    finally:
        if dbManager:
            dbManager.closeConnection()

if __name__ == "__main__":
    try:
        csv_file = "recruitment_data.csv"
        print("Starting data processing...")
        processDataFile(csv_file)
        print("Data processing completed successfully")
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
