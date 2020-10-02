from glob import glob

import pandas as pd

tableMap = {
    'B01001': 'Sex By Age',  # too large
    'B01002': 'Median Age by Sex',
    'B01003': 'Total Population',
    'B02001': 'Race',
    'B11005': 'HOUSEHOLDS BY PRESENCE OF PEOPLE UNDER 18 YEARS BY HOUSEHOLD TYPE',  # identifier , col name too large
    'B17001': 'POVERTY STATUS IN THE PAST 12 MONTHS BY SEX BY AGE',  # too large  #identifier , col name too large
    'B18101': 'SEX BY AGE BY DISABILITY STATUS',
    'B19001': 'HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2018 INFLATION-ADJUSTED DOLLARS)',
    'B19013': 'MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2018 INFLATION-ADJUSTED DOLLARS) (WHITE ALONE HOUSEHOLDER)',
    'B19083': 'GINI INDEX OF INCOME INEQUALITY',
    'B27001': 'HEALTH INSURANCE COVERAGE STATUS BY SEX BY AGE',  # TO0 LARGE DATA
    'B28005': 'AGE BY PRESENCE OF A COMPUTER AND TYPES OF INTERNET SUBSCRIPTION IN HOUSEHOLD',  # has data from 2013
    # 'S1903': 'MEDIAN INCOME IN THE PAST 12 MONTHS (IN 2018 INFLATION-ADJUSTED DOLLARS)' # internal files has diff starting name like ACSST1
}
Years = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018']


class CensusDataUtils:
    def __init__(self, mysqlClient, verbose=0):
        try:
            self.verbose = verbose
            self.mysqlClient = mysqlClient
        except Exception as e:
            print(str(e))

    def load(self):
        meta_data_df = _get_metadata_df()
        self.mysqlClient.store_df_sql(meta_data_df, 'CensusMetadata')
        #
        cook_book_df = _get_code_book_df()
        self.mysqlClient.store_df_sql(cook_book_df, 'CodeBook')

        for tableId, tableTitle in tableMap.items():
            frame = _get_table_df(tableId, tableTitle, cook_book_df, self.verbose)
            self.mysqlClient.store_df_sql(frame, tableId)


def _get_metadata_df(verbose=0):
    filePath = './data/census2020/metadata/2018_DataProductList.xlsx'
    if verbose == 1:
        print('Loading {}'.format(filePath))
    # dfs = pd.read_excel(f"{filePath}", sheet_name=None, index_col=0, header=None)
    xl_file = pd.ExcelFile(f"{filePath}")
    dfs = {sheet_name: xl_file.parse(sheet_name) for sheet_name in xl_file.sheet_names}
    df = dfs['Data Product List']
    return df


def _get_table_df(tableId, tableTitle, cook_book_df, verbose=0):
    # pop_table = df.loc[df['Table ID'] == tableId]
    # tableTitle = pop_table.iat[0, 2]
    if verbose == 1:
        print('\nTABLE {}: {} '.format(tableId, tableTitle))
    li = []
    header = []
    for year in Years:
        filepath = './data/census2020/{}-{}/'.format(tableId, tableTitle)
        filename = '*{}.{}_data_with_overlays_*.csv'.format(year, tableId)
        # if verbose == 1:
        #     print('Loading  {}'.format(f"{filepath}{filename}"))
        for f in glob(f"{filepath}{filename}"):
            if verbose == 1:
                print('Loading  {}'.format(f))
            data = pd.read_csv(f, header=None)
            header = data.iloc[0].values.flatten().tolist()
            header = [x.strip(' ') for x in header]
            data = data.iloc[2:, :]
            data['Year'] = year
            li.append(data)
    frame = pd.concat(li, axis=0, ignore_index=True)
    # new_header = cook_book_df[cook_book_df.iloc[:, 0].isin(header)]
    # new_header_list = new_header.iloc[:, 2].values.flatten().tolist()
    # if verbose == 1:
    #     assert_code_book_cols(header, new_header_list, cook_book_df)

    header.append('Year')
    # new_header_list.append('Year')

    # if verbose == 1:
    #     print('New Header {}'.format(new_header_list))
    #     print('Header {}'.format(header))

    frame.columns = header
    return frame


def _assert_code_book_cols(old, new, code_book_df):
    print(old)
    print(new)
    print('SIZE\t  Old: {} & New {} \t Asert :  {} \t'.format(len(old), len(new), len(old) == len(new)))
    for i in range(len(old)):
        try:
            row = code_book_df.loc[code_book_df.iloc[:, 0] == old[i]]
            o = old[i]
            n = new[i]
            expected = row.iloc[:, 2].values[0]
            print('Old : {} \t New : {} Expected:{} \t Assert : {}'.format(o, n, expected, expected == n))
            assert (new[i] == n)
        except Exception as e:
            print('-----> ', row.iloc[:, 2])
            print(e)
            raise Exception('ColumnName not found in code_book for \'{}\''.format(o))


def _get_code_book_df(verbose=0):
    filePath = './data/census2020/codebook/CodeBook.*.xlsx'
    li = []
    for f in glob(f"{filePath}"):
        if verbose == 1:
            print('Loading {}'.format(f))
        dfs = pd.read_excel(f, sheet_name='Sheet1', index_col=None, header=None, na_values=['NA'])
        dfs = dfs.iloc[:, 0:3]
        # print('\n', f)
        # print('@@@@@: ', dfs[dfs.iloc[:, 2].isin(['Geo_Area_Name', 'Geo__Area__Name', 'Geographic_Area_Name', 'Geo_ Area_ Name'])])
        li.append(dfs)
    frame = pd.concat(li, axis=0, ignore_index=True)
    frame = frame.drop_duplicates(ignore_index=True)
    frame = frame.dropna()
    indexList = frame[frame.iloc[:, 2].isin(['Geo__Area__Name', 'Geographic_Area_Name', 'Geo_ Area_ Name'])].index
    frame.drop(indexList, inplace=True)
    # print(frame.loc[frame.loc[:, 0] == 'NAME'])
    frame.columns = ['CodeName', 'Description', 'ColumnName']
    return frame
