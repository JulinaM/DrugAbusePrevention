import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Text
from sqlalchemy.orm import sessionmaker


class MySQLModel:

    def __init__(self, connection, verbose=0):
        try:
            self.connection = connection
            self.verbose = verbose
        except Exception as e:
            print(str(e))

    def insert(self, df, tableName):
        df.to_sql(con=self.connection, name=tableName, if_exists='append', flavor='mysql')

    @staticmethod
    def store_df_sql(df, tableName):
        print('Writing to DB................')
        connectionString = "mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="password",
                                                                               db="census2020")
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

    @staticmethod
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

    @staticmethod
    def query_result():
        connectionString = "mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="password",
                                                                               db="census2020")
        engine = create_engine(connectionString)
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            meta = MetaData()
            OH_Hospitalization_Number = Table(
                'NY_Hospitalization_Number', meta,
                Column('Indicator', Text, primary_key=True),
                Column('Location', Text),
                Column('2018 Q4', Text),
            )

            for instance in session.query(OH_Hospitalization_Number):
                print(instance.location, instance['2018 Q4'])
            # s = OH_Hospitalization_Number.select()
            # conn = engine.connect()
            # result = conn.execute(s)
            # for res in result:
            #     print(res)

            # for row in result:
            #     print(row)
        finally:
            session.close()


if __name__ == '__main__':
    MySQLModel.query_result()
