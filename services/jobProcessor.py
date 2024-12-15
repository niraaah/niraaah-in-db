from typing import Dict, Optional, List
import pandas as pd
from utils.logConfig import setupLogger

logger = setupLogger()

class JobDataProcessor:
    def __init__(self, dbManager):
        self.dbManager = dbManager

    def processJobEntry(self, rowData: pd.Series) -> bool:
        try:
            companyId = self._processCompany(rowData['회사명'])
            locationId = self._processLocation(rowData['지역'])
            techStackIds = self._processTechStacks(rowData['직무분야'])
            categoryIds = self._processCategories(rowData['경력'])
            
            jobData = {
                'companyId': companyId,
                'jobTitle': rowData['제목'],
                'jobLink': rowData['링크'],
                'experienceLevel': None if pd.isna(rowData['경력']) else rowData['경력'],
                'educationLevel': None if pd.isna(rowData['학력']) else rowData['학력'],
                'employmentType': None if pd.isna(rowData['고용형태']) else rowData['고용형태'],
                'salaryInfo': None if pd.isna(rowData['연봉정보']) else rowData['연봉정보'],
                'locationId': locationId,
                'deadlineDate': None if pd.isna(rowData['마감일']) else rowData['마감일'],
                'techStacks': techStackIds,
                'categories': categoryIds
            }
            
            return self._insertJobPosting(jobData)
            
        except Exception as e:
            logger.error(f"Error processing job entry: {str(e)}")
            return False

    def _processCompany(self, companyName: str) -> Optional[int]:
        cursor = self.dbManager.dbCursor
        try:
            cursor.execute("SELECT company_id FROM companies WHERE name = %s", (companyName,))
            result = cursor.fetchone()
            
            if result:
                return result['company_id']
            
            cursor.execute("INSERT INTO companies (name) VALUES (%s)", (companyName,))
            self.dbManager.connection.commit()
            return cursor.lastrowid
        except Exception as e:
            self.dbManager.connection.rollback()
            raise
