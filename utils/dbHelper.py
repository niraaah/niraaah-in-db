databasePool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=32,  # 기본값보다 크게 설정
    **databaseConfig
) 