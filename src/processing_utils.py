import os
import itertools
import pandas as pd

def metro_extractor(metro_ls):
    stops_names = [x['name'] for x in metro_ls]
    lines_names = [x['lines'] for x in metro_ls]
    lines_names = set(itertools.chain.from_iterable(lines_names))
    return [stops_names, lines_names]

def cleaner(df, columns=['source_logo', 'source_label', 'search_type',
 'rent_max', 'bedroom', 'buy_type', 'new_real_estate', 'webview_link', 'source_description']):

    df['features'] = df['features'].apply(lambda x: {} if pd.isna(x) else x)
    df_extract = pd.json_normalize(df['features'])
    df = df.merge(df_extract, how='left', on='id')

    metro_res = df['stops'].apply(lambda x: metro_extractor(x))
    df[['metro_stations', 'metro_lines']] = pd.DataFrame(metro_res.tolist(), index=df.index)

    columns_to_drop = columns + ['year', 'box', 'stops', 'features']
    df = df.drop(columns=columns_to_drop)
    return df


def features_engineering(df):
    df['price_m2'] = df['rent'] / df['area']
    df['rent_evolution'] = df['previous_rent'] - df['rent']
    df['geo_coords'] = df['lat'].astype('string') + ', ' + df['lng'].astype('string')

    df = df.drop(columns=['previous_rent', 'lat', 'lng'])
    return df 

def append_history_df(df, history_path):
    if os.path.exists(history_path):
        df_history = pd.read_csv(history_path, encoding='utf-8', sep=',', index_col=['id'])
    else:
        df_history = pd.DataFrame(columns=df.columns)

    new_entries = set(df.index) - set(df_history.index)
    df_to_append = df.loc[new_entries, :]
    df_history = df_history.append(df_to_append)
    return df_history

def update_history_df(df, df_history, new_expired_list):

    updated_entries = df.loc[new_expired_list, :]
    df_history.loc[new_expired_list, 'true_expired_at'] = updated_entries['true_expired_at']
    return df_history
