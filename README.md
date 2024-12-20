# niraaah-in-db

사람인 채용정보 크롤링 및 데이터베이스 관리 프로젝트

## 프로젝트 구조

```
niraaah-in-db/
├── database/
│   └── connectionManager.py    # 데이터베이스 연결 관리
├── services/
│   ├── dataLoader.py          # 사람인 채용정보 크롤링
│   └── jobProcessor.py        # 채용정보 데이터 처리
├── utils/
│   └── logConfig.py           # 로깅 설정
├── app.py                     # 메인 애플리케이션
└── requirements.txt           # 의존성 패키지 목록
```

## 설치 방법

1. 가상환경 설정

```bash
# 가상환경 생성
python -m venv venv

# 가상환경 활성화
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

2. 필요한 패키지 설치

```bash
pip install -r requirements.txt
```

3. 데이터베이스 설정

```python
databaseConfig = {
    "host": "your_db_host",
    "port": your_db_port,
    "user": "your_db_user",
    "password": "your_db_password",
    "database": "your_db_name"
}
```

4. Chrome WebDriver 설치
- Selenium 사용을 위한 Chrome WebDriver가 필요합니다
- webdriver-manager가 자동으로 관리해줍니다

## 실행 방법

1. 가상환경 활성화 확인
```bash
# 터미널에 (venv)가 표시되어 있는지 확인
# 활성화가 안 되어있다면:
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

2. 개발 모드

```bash
python app.py
```

3. 프로덕션 모드

```bash
gunicorn -w 4 -b 0.0.0.0:10032 app:app
```

## 주요 기능

1. 채용정보 크롤링
- 사람인 채용공고 자동 수집
- Selenium을 이용한 동적 페이지 크롤링
- 정기적인 데이터 업데이트 (스케줄링)

2. 데이터 처리
- 채용공고 정보 정제 및 가공
- 기술 스택 분류
- 중복 데이터 처리

3. 데이터베이스 관리
- MySQL 데이터베이스 연결 관리
- 트랜잭션 처리
- 데이터 CRUD 작업

## 크롤링 정책

- 크롤링 간격: 1시간
- 동시 요청 제한: 3개
- 페이지당 처리 시간: 5초
- 재시도 횟수: 3회

## 에러 처리

- 크롤링 실패 시 로그 기록
- 데이터베이스 연결 오류 시 자동 재시도
- 중복 데이터 처리 로직