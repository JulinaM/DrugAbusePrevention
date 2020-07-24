import pandas as pd

from dbModels.MySQLModel import store_df_sql

QuarterMap = {
    'Jan-Mar': 'I',
    'Apr-Jun': 'II',
    'Jul-Sep': 'III',
    'Oct-Dec': 'IV',
}
Years = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018']


def load(verbose):
    ny_df = load_ny_data(verbose)
    store_df_sql(ny_df, 'NY_Hospitalization')

    ohio_dfs = load_ohio_data(verbose)
    store_df_sql(ohio_dfs, 'OH_Hospitalization')


def load_ohio_data(verbose=0):
    filePath = '../data/hospitalization_data/Ohio ED Visit Suspected Drug Overdose by County.xlsx'
    if verbose == 1:
        print('Loading {}'.format(filePath))
    df = pd.read_excel(filePath, sheet_name='Quarterly', index_col=None, skiprows=[0])
    df.rename(columns={df.columns[0]: "Location"}, inplace=True)
    if verbose == 1:
        print(df)
    return df


def load_ny_data(verbose=0):
    filePath = '../data/hospitalization_data/Aggergrated-NY-Hospitalization.xlsx'
    if verbose == 1:
        print('Loading {}'.format(filePath))
    dfs = pd.read_excel(filePath, sheet_name='Corrected Data', header=None, index_col=None)
    dfs = dfs.iloc[:, :-1]
    mux = pd.MultiIndex.from_arrays(dfs.ffill(1).values[:2, 1:], names=['col1', 'col2'])
    dfs = pd.DataFrame(dfs.values[2:, 1:], dfs.values[2:, 0], mux)
    dfs = dfs.reset_index()
    dfs.iloc[:, 0] = dfs.iloc[:, 0].ffill()

    if verbose == 1:
        print('---\n', dfs.head(10))

    dfs.columns = format_column(dfs.columns, verbose)
    print(dfs.head())
    return dfs


def format_column(columns, verbose):
    def date_lamda(col):
        first_col = col[0].strip(' ')
        if first_col != 'index' and first_col != 'Location':
            full_date = first_col.split(',')
            quarter = QuarterMap.get(full_date[0])
            column = '{}-{} {}'.format(quarter, full_date[1].strip(' '), col[1].strip(' '))
            return column
        else:
            return first_col

    new_column = list(map(lambda col: date_lamda(col), columns))
    if verbose == 1:
        print('Before: ', columns)
        print('After: ', new_column)
    return new_column


if __name__ == "__main__":
    load(1)
