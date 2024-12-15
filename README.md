# niraaah-in-db

사람인 채용정보 크롤링 및 데이터베이스 관리 프로젝트

## 프로젝트 구조

```
niraaah-in-db/
├── database/
│   └── connectionManager.py    # 데이터베이스 연결 관리
├── services/
│   ├── dataLoader.py           # 크롤링 데이터 로딩
│   └── jobProcessor.py         # 채용정보 처리
├── utils/
│   ├── logConfig.py            # 로깅 설정
├── main.py                     # 메인 실행 파일
├── requirements.txt            # 의존성 패키지 목록
└── recruitment_data.csv        # 크롤링된 데이터 저장 파일
```

## 설치 방법

1. 필요한 패키지 설치

```bash
pip install -r requirements.txt
```

2. 환경 변수 설정

- `.env.example` 파일을 참고하여 `.env` 파일 생성

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=recruitment
DB_USER=your_username
DB_PASSWORD=your_password
```

## 실행 방법

1. 크롤링 실행

```bash
python main.py
```

2. 데이터베이스 마이그레이션

```bash
python database/connectionManager.py
```

## 주요 기능

1. 데이터 크롤링

- 사람인 채용정보 자동 수집
- CSV 파일로 데이터 저장

2. 데이터베이스 관리

- PostgreSQL 데이터베이스 연결 관리
- 트랜잭션 처리
- 데이터 CRUD 작업