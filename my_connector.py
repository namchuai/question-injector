import mysql.connector
from mysql.connector import Error

class SqlWorker:
    def __init__(self, hostName, databaseName, userName, userPassword):
        self.hostName=hostName
        self.databaseName=databaseName
        self.userName=userName
        self.userPassword=userPassword

    def connect(self):
        try:
            self.connection=mysql.connector.connect(host=self.hostName, database=self.databaseName, user=self.userName, password=self.userPassword)
            if self.connection.is_connected():
                print("Connected to database {}".format(self.databaseName))
        except Error as e:
            print("Connection unsuccessful")

    def insert_part6(self,paragraph):
        try:
            cursor=self.connection.cursor()
            cursor.execute("INSERT INTO p6_tbl (paragraph) values ('{}')".format(paragraph))
            self.connection.commit()
            print("Question inserted successfully with id {}".format(cursor.lastrowid))
            cursor.close()
        except Error as e:
            print("Insert failed {}".format(e))

    def dispose(self):
        if self.connection.is_connected():
            self.connection.close()
            print("Connection closed")
