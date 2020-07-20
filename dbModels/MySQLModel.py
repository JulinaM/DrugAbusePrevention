import sqlalchemy
from sqlalchemy import create_engine


def store_df_sql(df, tableName):
    connectionString = "mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="password", db="census2020")
    engine = create_engine(connectionString)
    con = engine.connect()
    try:
        # dtype_dict = get_sql_col(df)
        df.to_sql(tableName, con, if_exists='append', index=False, chunksize=100)
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Table %s created successfully." % tableName);
    finally:
        con.close()


def get_sql_col(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})

        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})
    return dtypedict


class MySQLModel:

    def __init__(self, connection, verbose=0):
        try:
            self.connection = connection
            self.verbose = verbose
        except Exception as e:
            print(str(e))

    def insert(self, df, tableName):
        df.to_sql(con=self.connection, name=tableName, if_exists='append', flavor='mysql')
