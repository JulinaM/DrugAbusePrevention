# from sqlalchemy import create_engine
#
#
# def insert_DF_to_SQL(dataFrame, tableName, verbose=0):
#     connectionString = "mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="password", db="census")
#     engine = create_engine(connectionString)
#     dbConnection = engine.connect()
#     try:
#         dataFrame.to_sql(tableName, dbConnection, if_exists='append', chunksize=1000)
#     except ValueError as vx:
#         print(vx)
#     except Exception as ex:
#         print(ex)
#     else:
#         print("Table %s created successfully." % tableName);
#     finally:
#         dbConnection.close()


class MySQLModel:

    def __init__(self, connection, verbose=0):
        try:
            self.connection = connection
            self.verbose = verbose
        except Exception as e:
            print(str(e))

    def insert(self, df, tableName):
        df.to_sql(con=self.connection, name=tableName, if_exists='append', flavor='mysql')
