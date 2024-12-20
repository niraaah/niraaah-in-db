from typing import Dict, Optional, List
import pandas as pd
from utils.logConfig import setupLogger

logger = setupLogger()

class JobDataProcessor:
    def __init__(self, dbManager):
        self.dbManager = dbManager

    def processJobEntry(self, rowData: pd.Series) -> bool:
        try:
            # 회사 정보 처리
            companyId = self._processCompany(rowData['회사명'])
            
            # 위치 정보 처리 (시/구 분리)
            location = rowData['지역'].split()
            locationId = self._processLocation({
                'city': location[0] if location else None,
                'district': location[1] if len(location) > 1 else None
            })
            
            # 기술 스택 처리
            techStacks = [stack.strip() for stack in rowData['직무분야'].split(',')]
            techStackIds = self._processTechStacks(techStacks)
            
            # 경력 카테고리 처리
            categoryIds = self._processCategories([rowData['경력']])
            
            jobData = {
                'companyId': companyId,
                'jobTitle': rowData['제목'],
                'jobLink': rowData['링크'],
                'experienceLevel': rowData['경력'],
                'educationLevel': rowData['학력'],
                'employmentType': rowData['고용형태'],
                'salaryInfo': rowData['연봉'],  # CSV의 '연봉' 컬럼 사용
                'locationId': locationId,
                'deadlineDate': rowData['마감일'],
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

    def _processLocation(self, location: Dict) -> Optional[int]:
        """
        위치 정보를 처리하는 메서드입니다.
        현재는 사용하지 않으므로 None을 반환합니다.
        """
        return None

    def _processTechStacks(self, techStackStr: List[str]) -> List[int]:
        """기술 스택 문자열을 처리하여 tech_stack_id 리스트를 반환합니다."""
        if not techStackStr:
            return []
        
        techStackIds = []
        
        for tech in techStackStr:
            try:
                cursor = self.dbManager.dbCursor
                cursor.execute("SELECT stack_id FROM tech_stacks WHERE name = %s", (tech,))
                result = cursor.fetchone()
                
                if result:
                    techStackIds.append(result['stack_id'])
                else:
                    cursor.execute("INSERT INTO tech_stacks (name) VALUES (%s)", (tech,))
                    self.dbManager.connection.commit()
                    techStackIds.append(cursor.lastrowid)
            except Exception as e:
                self.dbManager.connection.rollback()
                logger.error(f"Error processing tech stack {tech}: {str(e)}")
        
        return techStackIds

    def _processCategories(self, categoryStr: List[str]) -> List[int]:
        """카테고리 문자열을 처리하여 category_id 리스트를 반환합니다."""
        if not categoryStr:
            return []
        
        categoryIds = []
        
        for category in categoryStr:
            try:
                cursor = self.dbManager.dbCursor
                cursor.execute("SELECT category_id FROM job_categories WHERE name = %s", (category,))
                result = cursor.fetchone()
                
                if result:
                    categoryIds.append(result['category_id'])
                else:
                    cursor.execute("INSERT INTO job_categories (name) VALUES (%s)", (category,))
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
                    employment_type, salary_info, location_id, deadline_date, job_link
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
                    "INSERT INTO job_tech_stacks (posting_id, stack_id) VALUES (%s, %s)",
                    (posting_id, tech_id)
                )
            
            self.dbManager.connection.commit()
            return True
            
        except Exception as e:
            self.dbManager.connection.rollback()
            logger.error(f"Error inserting job posting: {str(e)}")
            return False
