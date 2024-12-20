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

def loadTechnologyData(dbManager):
    try:
        cursor = dbManager.dbCursor
        
        # 기술 스택 데이터 로드
        tech_data = pd.read_csv('data/tech_stacks.csv')
        
        for _, row in tech_data.iterrows():
            cursor.execute(
                "INSERT INTO tech_stacks (stack_name, category) VALUES (%s, %s)",
                (row['name'], row.get('category', 'Other'))
            )
        
        dbManager.connection.commit()
        print("Technology data loaded successfully")
        
    except Exception as e:
        dbManager.connection.rollback()
        print(f"Error loading technology data: {e}")
        raise

def loadCategoryData(dbManager):
    try:
        cursor = dbManager.dbCursor
        
        # 직무 카테고리 데이터 로드
        category_data = pd.read_csv('data/job_categories.csv')
        
        for _, row in category_data.iterrows():
            cursor.execute(
                "INSERT INTO job_categories (name) VALUES (%s)",
                (row['name'],)
            )
        
        dbManager.connection.commit()
        print("Category data loaded successfully")
        
    except Exception as e:
        dbManager.connection.rollback()
        print(f"Error loading category data: {e}")
        raise

if __name__ == "__main__":
    try:
        csv_file = "recruitment_data.csv"  # CSV 파일 경로
        processDataFile(csv_file)
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
