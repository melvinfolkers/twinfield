import pandas as pd


def format_030_1(df):

    df.columns = [x.replace('fin.trs.', '') for x in df.columns]

    # format dates

    df['head.date'] = pd.to_datetime(df['head.date'], format='%Y%m%d')
    df['head.inpdate'] = pd.to_datetime(df['head.inpdate'], format='%Y%m%d%H%M%S')

    # format numbers
    df['line.basevaluesigned'] = df['line.basevaluesigned'].astype(float)
    df['line.valuesigned'] = df['line.valuesigned'].astype(float)
    df['line.repvaluesigned'] = df['line.repvaluesigned'].astype(float)
    df['line.vatbasevaluesigned'] = df['line.vatbasevaluesigned'].astype(float)
    df['line.quantity'] = df['line.quantity'].astype(float)

    return df