def store_df_sql(df, tableName):
    from sqlalchemy import create_engine
    connectionString = "mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="password", db="census2020")
    engine = create_engine(connectionString)
    con = engine.connect()
    try:
        df.to_sql(tableName, con, if_exists='append', chunksize=1000)
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Table %s created successfully." % tableName);
    finally:
        con.close()


class MySQLModel:

    def __init__(self, connection, verbose=0):
        try:
            self.connection = connection
            self.verbose = verbose
        except Exception as e:
            print(str(e))

    def insert(self, df, tableName):
        df.to_sql(con=self.connection, name=tableName, if_exists='append', flavor='mysql')
