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
            cursor.execute("SELECT company_id FROM companies WHERE company_name = %s", (companyName,))
            result = cursor.fetchone()
            
            if result:
                return result['company_id']
            
            cursor.execute("INSERT INTO companies (company_name) VALUES (%s)", (companyName,))
            self.dbManager.connection.commit()
            return cursor.lastrowid
        except Exception as e:
            self.dbManager.connection.rollback()
            raise

    def _processLocation(self, location: str) -> Optional[int]:
        """
        위치 정보를 처리하는 메서드입니다.
        현재는 사용하지 않으므로 None을 반환합니다.
        """
        return None

    def _processTechStacks(self, techStackStr: str) -> List[int]:
        """기술 스택 문자열을 처리하여 tech_stack_id 리스트를 반환합니다."""
        if pd.isna(techStackStr):
            return []
        
        techStacks = [tech.strip() for tech in techStackStr.split(',')]
        techStackIds = []
        
        for tech in techStacks:
            try:
                cursor = self.dbManager.dbCursor
                cursor.execute("SELECT stack_id FROM tech_stacks WHERE stack_name = %s", (tech,))
                result = cursor.fetchone()
                
                if result:
                    techStackIds.append(result['stack_id'])
                else:
                    cursor.execute(
                        "INSERT INTO tech_stacks (stack_name, category) VALUES (%s, 'Other')",
                        (tech,)
                    )
                    self.dbManager.connection.commit()
                    techStackIds.append(cursor.lastrowid)
            except Exception as e:
                self.dbManager.connection.rollback()
                logger.error(f"Error processing tech stack {tech}: {str(e)}")
        
        return techStackIds

    def _processCategories(self, categoryStr: str) -> List[int]:
        """카테고리 문자열을 처리하여 category_id 리스트를 반환합니다."""
        if pd.isna(categoryStr):
            return []
        
        categories = [cat.strip() for cat in categoryStr.split(',')]
        categoryIds = []
        
        for category in categories:
            try:
                cursor = self.dbManager.dbCursor
                cursor.execute("SELECT category_id FROM job_categories WHERE category_name = %s", (category,))
                result = cursor.fetchone()
                
                if result:
                    categoryIds.append(result['category_id'])
                else:
                    cursor.execute(
                        "INSERT INTO job_categories (category_name) VALUES (%s)",
                        (category,)
                    )
                    self.dbManager.connection.commit()
                    categoryIds.append(cursor.lastrowid)
            except Exception as e:
                self.dbManager.connection.rollback()
                logger.error(f"Error processing category {category}: {str(e)}")
        
        return categoryIds

    def _insertJobPosting(self, jobData: Dict) -> bool:
        """채용공고 정보를 데이터베이스에 저장합니다."""
        try:
            cursor = self.dbManager.dbCursor
            
            # 채용공고 기본 정보 저장
            query = """
                INSERT INTO job_postings (
                    title, company_id, experience_level, education_level,
                    employment_type, salary_range, location_id, deadline_date, job_link
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                jobData['jobTitle'], jobData['companyId'], jobData['experienceLevel'],
                jobData['educationLevel'], jobData['employmentType'], jobData['salaryInfo'],
                jobData['locationId'], jobData['deadlineDate'], jobData['jobLink']
            )
            
            cursor.execute(query, values)
            posting_id = cursor.lastrowid
            
            # 기술 스택 연결 정보 저장
            for tech_id in jobData['techStacks']:
                cursor.execute(
                    "INSERT INTO posting_tech_stacks (posting_id, stack_id) VALUES (%s, %s)",
                    (posting_id, tech_id)
                )
            
            # 카테고리 연결 정보 저장
            for category_id in jobData['categories']:
                cursor.execute(
                    "INSERT INTO posting_categories (posting_id, category_id) VALUES (%s, %s)",
                    (posting_id, category_id)
                )
            
            self.dbManager.connection.commit()
            return True
            
        except Exception as e:
            self.dbManager.connection.rollback()
            logger.error(f"Error inserting job posting: {str(e)}")
            return False
