CLUSTER_HEADLINE = "[[CLUSTER"


def create_mysql_connection():
    import mysql.connector
    return mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="toor",
        database="mpss2019"
    )
