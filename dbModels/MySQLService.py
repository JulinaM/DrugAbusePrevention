import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'Census2020'

TABLES = {}

TABLES['census_2010'] = (
    "CREATE TABLE `census` ("
    "  `geoId` varchar(55) NOT NULL,"
    "  `SUMLEV` varchar(55) NOT NULL,"
    "  `STATE` varchar(55) NOT NULL,"
    "  `COUNTY` varchar(55) NOT NULL,"
    "  `POP100` int(11) NOT NULL,"
    "  `NAME` varchar(55) NOT NULL,"
    # "  `gender` enum('M','F') NOT NULL,"
    "  PRIMARY KEY (`geoId`)"
    ") ENGINE=InnoDB")


class MySQLService:

    def __init__(self, host, user, passowrd, db=DB_NAME):
        try:
            self.connection = mysql.connector.connect(user=user, password=passowrd, host=host, db=db,
                                                      auth_plugin='mysql_native_password')
            # self.create_database(self.connection.cursor())
            # self.create_table(self.connection.cursor())

        except Exception as e:
            print(str(e))

    def create_database(self, cursor):
        try:
            cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    def create_tables(self, cursor):
        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")

        cursor.close()

    def close(self):
        self.connection.close()
