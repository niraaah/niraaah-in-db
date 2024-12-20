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
        dataFrame = pd.read_csv(filename, parse_dates=['수집일자', '마감일'])
        successCount = 0
        skipCount = 0
        
        # 각 행 처리
        for _, row in dataFrame.iterrows():
            try:
                # 데이터 매핑
                jobData = {
                    '회사명': row['회사명'],
                    '제목': row['제목'],
                    '링크': row['링크'],
                    '경력': row['경력'],
                    '학력': row['학력'],
                    '고용형태': row['고용형태'],
                    '연봉정보': row['연봉정보'],
                    '지역': row['지역'],
                    '직무분야': row['직무분야'],  # 기술 스택으로 사용
                    '마감일': row['마감일']
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
        processDataFile(csv_file)
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
