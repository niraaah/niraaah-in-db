import pandas as pd
from database.connectionManager import DatabaseManager
from services.jobProcessor import JobDataProcessor
from utils.logConfig import setupLogger

logger = setupLogger()

def processDataFile(filename: str):
    dbManager = DatabaseManager()
    processor = JobDataProcessor(dbManager)
    
    try:
        dataFrame = pd.read_csv(filename)
        successCount = 0
        skipCount = 0
        
        for _, rowData in dataFrame.iterrows():
            try:
                result = processor.processJobEntry(rowData)
                if result:
                    successCount += 1
                else:
                    skipCount += 1
                
            except Exception as e:
                logger.error(f"Row processing error: {str(e)}")
                continue
        
        logger.info(f"Processing completed. Inserted: {successCount}, Skipped: {skipCount}")
        
    except Exception as e:
        logger.error(f"File processing error: {str(e)}")
        raise
    finally:
        dbManager.closeConnection()
