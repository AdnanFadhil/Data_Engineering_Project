import psycopg2

class DBConfig:
    HOST = "localhost"
    PORT = "5433"
    DBNAME = "project_capstone"
    USER = "postgres"
    PASSWORD = "postgres"

    @staticmethod
    def get_connection():
        return psycopg2.connect(
            host=DBConfig.HOST,
            port=DBConfig.PORT,
            database=DBConfig.DBNAME,
            user=DBConfig.USER,
            password=DBConfig.PASSWORD
        )
