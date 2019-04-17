import pandas as pd


def format_030_1(df):

    df.columns = [x.replace('fin.trs.', '') for x in df.columns]

    # format dates
    if 'head.date' in df.columns: df['head.date'] = pd.to_datetime(df['head.date'], format='%Y%m%d')
    if 'head.inpdate' in df.columns: df['head.inpdate'] = pd.to_datetime(df['head.inpdate'], format='%Y%m%d%H%M%S')

    # format numbers
    numbers = ['line.basevaluesigned','line.valuesigned','line.repvaluesigned','line.vatbasevaluesigned', 'line.quantity']

    for column in numbers:
        if column in df.columns: df[column] = df[column].astype(float)

    return df

def format_164(df):

    df.columns = [x.replace('fin.trs.', '') for x in df.columns]

    # format dates

    # format numbers


    return df