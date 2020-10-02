import pandas as pd

QuarterMap = {
    'Jan-Mar': 'Q1',
    'Apr-Jun': 'Q2',
    'Jul-Sep': 'Q3',
    'Oct-Dec': 'Q4',
}
Years = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018']


class HospitalizationDataUtils:

    def __init__(self, mysqlClient, verbose=0):
        try:
            self.verbose = verbose
            self.mysqlClient = mysqlClient
        except Exception as e:
            print(str(e))

    def load(self):
        NY_number_df, NY_crude_rate_df = _load_NY_data(self.verbose)
        self.mysqlClient.store_df_sql(NY_number_df, 'NY_Hospitalization_Number')
        self.mysqlClient.store_df_sql(NY_crude_rate_df, 'NY_Hospitalization_Rate')

        OH_dfs = _load_OH_data(self.verbose)
        self.mysqlClient.store_df_sql(OH_dfs, 'OH_Hospitalization_Number')


def _load_OH_data(verbose=0):
    filePath = './data/hospitalization_data/Ohio ED Visit Suspected Drug Overdose by County.xlsx'
    if verbose == 1:
        print('Loading {}'.format(filePath))
    df = pd.read_excel(filePath, sheet_name='Quarterly', index_col=None, skiprows=[0])
    df.rename(columns={df.columns[0]: "Location"}, inplace=True)
    if verbose == 1:
        print(df)
    return df


def _load_NY_data(verbose=0):
    filePath = './data/hospitalization_data/Aggergrated-NY-Hospitalization.xlsx'
    if verbose == 1:
        print('Loading {}'.format(filePath))
    dfs = pd.read_excel(filePath, sheet_name='Corrected Data', header=None, index_col=None)
    dfs = dfs.iloc[:, :-1]  # drop last redundant column
    mux = pd.MultiIndex.from_arrays(dfs.ffill(1).values[:2, 1:], names=['col1', 'col2'])
    dfs = pd.DataFrame(dfs.values[2:, 1:], dfs.values[2:, 0], mux)
    dfs = dfs.reset_index()
    dfs.iloc[:, 0] = dfs.iloc[:, 0].ffill()

    dfs.rename(columns={'index': 'Indicator'}, inplace=True)
    if verbose == 1:
        print('---\n', dfs.head(10))

    even_df = dfs.iloc[:, ::2]  # even
    odd_df = dfs.iloc[:, 1::2]  # odd
    even_df.insert(1, 'Location', dfs.loc[:, 'Location'].values)
    odd_df.insert(0, 'Indicator', dfs.loc[:, 'Indicator'].values)

    even_df.columns = _format_column(even_df.columns, verbose)
    odd_df.columns = _format_column(odd_df.columns, verbose)
    if verbose == 1:
        print(even_df.head(10))
        print(odd_df.head(10))
    return even_df, odd_df


def _format_column(columns, verbose):
    def date_lamda(col):
        first_col = col[0].strip(' ')
        if first_col != 'Indicator' and first_col != 'Location':
            full_date = first_col.split(',')
            quarter = QuarterMap.get(full_date[0])
            column = '{} {}'.format(full_date[1].strip(' '), quarter)
            return column
        else:
            return first_col

    new_column = list(map(lambda col: date_lamda(col), columns))
    if verbose == 1:
        print('Before: ', columns)
        print('After: ', new_column)
    return new_column

