import sys
import mysql
from mysql.connector import Error
import psycopg2
import time


DB_KIND = "postgres"
DB_NAME = "quay"
HOST = "localhost"
USERNAME = "quay"
PASSWORD = "quay"



REPOSITORY_NAMESPACE_ID = 1
SIZE = 2

def create_sql_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            passwd=db_password,
            database=db_name
        )
        print("Connection to MySQL successful")
    except Error as e:
        print(f"Error creating connection: '{e}'")
        sys.exit(1)

    return connection

def create_postgres_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name, user=db_user, password=db_password, host=db_host, port=db_port
        )
        print("Connection to postgres successful")
    except Error as e:
        print(f"Error creating connection: '{e}'")
        sys.exit(1)
    return connection

def verify_repositories(cursor):
    print("================== REPOSITORIES ==================")
    cursor.execute("select * from quotarepositorysize join repository on repository.id=quotarepositorysize.repository_id ")
    for size in cursor.fetchall():
        calculated_size = size[SIZE]

        # Calculate total repository size
        cursor.execute(f"""
        select sum(image_size) from (    
            SELECT imagestorage.image_size, imagestorage.id FROM manifestblob            
            JOIN imagestorage ON manifestblob.blob_id = imagestorage.id     
            WHERE manifestblob.repository_id={str(size[REPOSITORY_NAMESPACE_ID])}     
            group by imagestorage.id 
        ) a
        """)
        expected_size = cursor.fetchone()
        expected_size = expected_size[0]

        if expected_size is None:
            expected_size = 0
        if calculated_size is None:
            calculated_size = 0

        if calculated_size != expected_size:
            print("X incorrect value for repo "+str(size[REPOSITORY_NAMESPACE_ID])+" expected "+str(expected_size)+" but got "+str(calculated_size))
        else:
            print("✓ Correct value for repo "+str(size[REPOSITORY_NAMESPACE_ID])+" "+str(calculated_size))

def verify_namespaces(cursor):
    print("================== NAMESPACES ==================")
    cursor.execute("select * from quotanamespacesize")
    for size in cursor.fetchall():
        calculated_size = size[SIZE]

        # Calculate total namespace size
        cursor.execute(f"""
        select sum(image_size) from (     
            SELECT imagestorage.image_size, imagestorage.id FROM manifestblob            
            JOIN repository ON manifestblob.repository_id = repository.id
            JOIN imagestorage on manifestblob.blob_id = imagestorage.id
            WHERE repository.namespace_user_id={str(size[REPOSITORY_NAMESPACE_ID])}
            group by imagestorage.id 
        ) a""")
        expected_size = cursor.fetchone()
        expected_size = expected_size[0]

        if expected_size is None:
            expected_size = 0
        if calculated_size is None:
            calculated_size = 0

        if calculated_size != expected_size:
            print("X incorrect value for namespace "+str(size[REPOSITORY_NAMESPACE_ID])+" expected "+str(expected_size)+" but got "+str(calculated_size))
        else:
            print("✓ Correct value for namespace "+str(size[REPOSITORY_NAMESPACE_ID])+" "+str(calculated_size))

if __name__ == "__main__":
    if DB_KIND == "postgres":
        conn = create_postgres_connection(DB_NAME,USERNAME,PASSWORD,HOST,5432)
        cursor = conn.cursor()
    elif DB_KIND == "mysql":
        conn =  create_sql_connection(DB_NAME,USERNAME,PASSWORD,HOST,3306)
        cursor = conn.cursor(buffered=True)

    verify_repositories(cursor)
    verify_namespaces(cursor)

    conn.close()
