import os
import itertools
import pandas as pd
import numpy as np
import json


USER_TOKEN_FILE = os.path.join('databases', 'credentials.json')


def metro_extractor(metro_ls):
    stops_names = [x['name'] for x in metro_ls]
    lines_names = [x['lines'] for x in metro_ls]
    lines_names = set(itertools.chain.from_iterable(lines_names))
    return [stops_names, lines_names]

def cleaner(df, columns=None):

    if columns is None:
        columns = ['source_logo', 'source_label', 'search_type',
                    'rent_max', 'bedroom', 'buy_type', 'new_real_estate', 'webview_link', 'source_description']

    df['features'] = df['features'].apply(lambda x: {} if pd.isna(x) else x)
    df_extract = pd.json_normalize(df['features'])
    df = df.merge(df_extract, how='left', on='id')

    metro_res = df['stops'].apply(lambda x: metro_extractor(x))
    closest_metro = df['stops'].apply(lambda x: metro_extractor(x))
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


def metro_geo_pos_when_none(df):
    df["closest_metro"] = df['metro_stations'].str[0]
    df_metro_coords = pd.read_csv(
        'databases/emplacement-des-gares-idf.csv',
        sep=';',
        usecols=['Geo Point', 'nom'])
    df['is_real_coord'] = np.where(df['geo_coords'].isna(), 0, 1)
    for i in range(df.shape[0]):
        if df.iloc[i]['is_real_coord'] == 0:
            closest_metro = df.iloc[i]['closest_metro']
            pos_closest_metro = df_metro_coords.loc[df_metro_coords['nom'] == closest_metro, 'Geo Point']
            if len(pos_closest_metro) > 0:
                pos_closest_metro = add_noise_to_metro_pos(pos_closest_metro)
                df.at[i, 'geo_coords'] = pos_closest_metro
            else:
                df.at[i, 'geo_coords'] = f'48.866667, 2.333333'
    return df


def add_noise_to_metro_pos(series_pos_metro: pd.Series):
    arr = np.array(series_pos_metro.values[0].split(',')).astype('float64')
    arr += np.random.normal(0, 1e-3, 2)
    arr = np.around(arr, decimals=4)
    return ', '.join(arr.astype(str))


def all_sharing_links(df, user_token):
    df['sharing_link'] = df.apply(lambda row: good_link_for_sharing(row['id'], user_token), axis=1)
    return df


def good_link_for_sharing(ad, user_token):
    good_link = f'https://www.jinka.fr/alert_result?' \
                 f'token={user_token}&ad={ad}&from=dashboard_card&from_alert_filter=all&from_alert_page=1'
    return good_link
