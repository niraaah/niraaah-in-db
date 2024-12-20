from typing import Dict, Optional, List
import pandas as pd
from utils.logConfig import setupLogger

logger = setupLogger()

class JobDataProcessor:
    def __init__(self, dbManager):
        self.dbManager = dbManager

    def processJobEntry(self, rowData: Dict) -> bool:
        try:
            companyId = self._processCompany(rowData['company_name'])
            techStackIds = self._processTechStacks(rowData['tech_stack'])
            categoryIds = self._processCategories(rowData['experience'])
            
            jobData = {
                'companyId': companyId,
                'title': rowData['title'],
                'link': rowData['link'],
                'experience': None if pd.isna(rowData['experience']) else rowData['experience'],
                'education': None if pd.isna(rowData['education']) else rowData['education'],
                'employment_type': None if pd.isna(rowData['employment_type']) else rowData['employment_type'],
                'salary': None if pd.isna(rowData['salary']) else rowData['salary'],
                'location': rowData['location'],
                'deadline': None if pd.isna(rowData['deadline']) else rowData['deadline'],
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
        query = """
            INSERT INTO job_postings (
                company_id, title, experience_level, education_level,
                employment_type, salary_range, location_city, location_district,
                deadline_date, job_link
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 지역 분리
        location_parts = jobData['location'].split() if jobData['location'] else [None, None]
        city = location_parts[0] if len(location_parts) > 0 else None
        district = location_parts[1] if len(location_parts) > 1 else None
        
        values = (
            jobData['companyId'],
            jobData['title'],
            jobData['experience'],
            jobData['education'],
            jobData['employment_type'],
            jobData['salary'],
            city,
            district,
            jobData['deadline'],
            jobData['link']
        )
        
        try:
            cursor = self.dbManager.dbCursor
            
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
