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

if __name__ == "__main__":
    try:
        csv_file = "recruitment_data.csv"  # CSV 파일 경로
        processDataFile(csv_file)
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
