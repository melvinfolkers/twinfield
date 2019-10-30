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


def maak_samenvatting(df):
    agg = df.groupby(
        ['administratienaam', 'head.year', 'head.period', 'head.status', 'line.dim1', 'line.dim1name', 'line.dim1type',
         'line.dim2', 'line.dim2name'])['line.valuesigned'].sum().reset_index()

    fieldmapping = {'head.year': 'Jaar',
                    'head.period': 'Periode',
                    'head.status': 'Status',
                    'head.relationname': 'Relatienaam',
                    'line.dim1': 'Grootboekrek.',
                    'line.dim1type': 'Grootboektype',
                    'line.dim1name': 'Grootboekrek.naam',
                    'line.dim2': 'Kpl./rel.',
                    'line.dim2name': 'Kpl.-/rel.naam',
                    'line.valuesigned': 'Bedrag'}

    agg.rename(columns=fieldmapping, inplace=True)

    return agg