import pandas as pd
import requests
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm, trange
from logzero import logger


def authenticate(email, password):
    auth_url: str = 'https://api.jinka.fr/apiv2/user/auth'
    auth_dict: str = {'email': email, 'password': password}
    s = requests.Session()
    r_auth = s.post(auth_url, auth_dict)
    if r_auth.status_code == 200:
        logger.info('Authentification succeeded (200)')
        access_token = r_auth.json()['access_token']
    else:
        logger.critical(f'Authentication failed with error {r_auth.status_code}')
        return None, None

    headers = {
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'Origin': 'https://www.jinka.fr',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Sec-GPC': '1',
        'If-None-Match': 'W/f46-qWZd5Nq9sjWAv9cj3oEhFaxFuek',
        'TE': 'Trailers',
    }

    return s, headers


def get_alerts(session, headers):
    r_alerts = session.get('https://api.jinka.fr/apiv2/alert', headers=headers)
    data_dict = {
        'id': [], 'name': [], 'user_name': [], 'ads_per_day': [], 'nb_pages': [], 'all': [], 'read': [],
        'unread': [], 'favorite': [], 'contact': [], 'deleted': []}

    for counter, alert in enumerate(r_alerts.json()):
        data_dict['id'].append(alert['id'])
        data_dict['name'].append(alert['name'])
        data_dict['user_name'].append(alert['user_name'])
        data_dict['ads_per_day'].append(alert['estimated_ads_per_day'])

        root_url = 'https://api.jinka.fr/apiv2/alert/' + str(alert['id']) + '/dashboard'

        r_pagination = session.get(root_url, headers=headers)
        pagination_data = r_pagination.json()['pagination']
        data_dict['nb_pages'].append(pagination_data['nbPages'])
        data_dict['all'].append(pagination_data['totals']['all'])
        data_dict['read'].append(pagination_data['totals']['read'])
        data_dict['unread'].append(pagination_data['totals']['unread'])
        data_dict['favorite'].append(pagination_data['totals']['favorite'])
        data_dict['contact'].append(pagination_data['totals']['contact'])
        data_dict['deleted'].append(pagination_data['totals']['deleted'])

        logger.info(f'{counter+1} / {len(r_alerts.json())} alerts have been processed.')

    df_alerts = pd.DataFrame(data=data_dict)
    return df_alerts


def get_appart_response(session, row_tuple):

    alert_id = row_tuple[1]['alert_id']
    appart_id = str(row_tuple[0])

    headers = {
        'authority': 'api.jinka.fr',
        'upgrade-insecure-requests': '1',
        'dnt': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/88.0.4324.190 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,'
                  'image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    params = (
        ('ad', appart_id),
        ('alert_token', alert_id),
    )
    try:
        response = session.get('https://api.jinka.fr/alert_result_view_ad', headers=headers, params=params)
    except:
        logger.warn('Connection interrupted by Jinka. Waiting 30 seconds before retrying.')
        time.sleep(30)
        logger.warn('Retrying to establish the connection...')
        response = session.get('https://api.jinka.fr/alert_result_view_ad', headers=headers, params=params)
    return response


def expired_checker(response, row_tuple):

    source = row_tuple[1]['source']
    true_expired_date = None

    #TODO: clean if-ellifs in more readable blocks

    if source in ['logic-immo', 'century21', 'meilleursagents', 'locservice', 'lagenceblue']:
        parsed_url = BeautifulSoup(response.text, 'html.parser')
    elif source in ['pap', 'seloger', 'paruvendu', 'laforet', 'orpi', 'avendrealouer', 'fnaim', 'locatair']:
        parsed_url = response.url.split('/')
    elif source == 'leboncoin':
        true_expired_date = row_tuple[1]['expired_at']
    else:
        return true_expired_date

    if source == 'logic-immo':
        item = parsed_url.find_all(class_="expiredTxt")
        if len(item) != 0:
            true_expired_date = datetime.now()

    if source == 'pap':
        if parsed_url[3] == 'annonce':
            true_expired_date = datetime.now()

    if source == 'seloger':
        if parsed_url[-1] == '#expiree':
            true_expired_date = datetime.now()

    if source == 'explorimmo':
        pass

    if source == 'paruvendu':
        if parsed_url[-1] == '#showError404':
            true_expired_date = datetime.now()

    if source == 'century21':
        item = parsed_url.find_all(class_="content_msg")
        item2 = parsed_url.find_all(class_="tw-font-semibold tw-text-lg")
        if len(item)!=0:
            if item[0].strong.text == "Nous sommes désolés, la page à laquelle vous tentez d'accéder n'existe pas.":
                true_expired_date = datetime.now()
        if len(item2)!=0:
            if item2[0].text.strip() == "Cette annonce est désactivée, retrouvez ci-dessous une sélection de biens s'en rapprochant.":
                true_expired_date = datetime.now()

    if source == 'stephaneplaza':
        pass

    if source == 'meilleursagents':
        item = parsed_url.find_all(class_="error-page")
        if len(item)!=0:
            true_expired_date = datetime.now()

    if source == 'flatlooker':
        pass

    if source == 'bienici':
        pass

    if source == 'locservice':
        item = parsed_url.find_all(class_="louerecemment")
        if len(item) != 0:
            true_expired_date = datetime.now()

    if source ==  'guyhoquet':
        pass

    if source == 'laforet':
        if parsed_url[3] == 'ville':
            true_expired_date = datetime.now()

    if source == 'lagenceblue':
        item = parsed_url.find_all(class_="label label-warning")
        if len(item) != 0:
            true_expired_date = datetime.now()

    if source == 'avendrealouer':
        if '#expiree' in parsed_url[-1]:
           true_expired_date = datetime.now()

    if source == 'orpi':
        if parsed_url[-2] == 'louer-appartement':
           true_expired_date = datetime.now()

    if source == 'parisattitude':
        pass

    if source == 'fnaim':
        if len(parsed_url) >= 3:
            if parsed_url[3] != 'annonce-immobiliere':
                true_expired_date = datetime.now()

    if source == 'erafrance':
        pass

    return true_expired_date


def get_all_links(session, df, expired, appart_db_path):

    new_expired_list = []

    if os.path.exists(appart_db_path) and (expired == False):
        logger.info('Found a preexisting links database.')
        df['link'] = None
        df['true_expired_at'] = None
        df_already_processed = pd.read_json(appart_db_path, orient='columns')

        unprocessed_index = set(df.index) - set(df_already_processed.index)
        processed_index = set(df.index).intersection(df_already_processed.index)
        df.loc[processed_index, 'link'] = df_already_processed['link']
        df.loc[processed_index, 'true_expired_at'] = df_already_processed['true_expired_at']

    else:
        if os.path.exists(appart_db_path) == False:
            logger.warn('No preexisting database has been found, generating a new one.')
        elif expired:
            logger.warn('Replacing the previous database in order to check for apparts expiration.')
        unprocessed_index = df.index
        df_already_processed = pd.DataFrame()

    logger.info(f'{len(unprocessed_index)} new links have been detected.')

    if len(unprocessed_index) != 0:
        links = list(unprocessed_index.copy())
        expiration_list = list(unprocessed_index.copy())

        idx = 0
        for row_tuple in tqdm(df.iterrows(), total=len(df)):

            response = get_appart_response(session, row_tuple)
            true_expiration_date = expired_checker(response, row_tuple)
            true_url = response.url
            links[idx] = true_url
            expiration_list[idx] = true_expiration_date
            if true_expiration_date is not None:
                new_expired_list.append(row_tuple[0])
            idx += 1

        df.loc[unprocessed_index, 'link'] = links
        df.loc[unprocessed_index, 'true_expired_at'] = expiration_list

        df_to_append = df.loc[unprocessed_index, ['link', 'true_expired_at']]

        df_already_processed = df_already_processed.append(df_to_append)
        df_already_processed.to_json(appart_db_path, orient='columns')

        nb_expired = len(df[df['true_expired_at'].notna()])
        logger.warn(f'{nb_expired} appartments have expired.')

    return df, new_expired_list


def remove_expired(session, df, new_expired_list, last_deleted_path):

    df_expired = df.loc[new_expired_list, :]

    logger.info('Starting the cleaning of expired offers.')

    for appart_id, row in tqdm(df_expired.iterrows()):
        post_url = 'https://api.jinka.fr/apiv2/alert/' + row['alert_id'] + '/abuses'
        data = {'ad_id':appart_id, 'reason':'ad_link_404'}
        session.post(post_url, data=data)


    df_expired.to_json(last_deleted_path, orient='columns')
    cleaned_df = df[df['true_expired_at'].isna()]

    logger.info(f'Finished cleaning the {len(new_expired_list)} expired appartments.')
    return cleaned_df


def get_apparts(session, headers, alert_id, nb_pages):
    root_url = 'https://api.jinka.fr/apiv2/alert/' + str(alert_id) + '/dashboard'

    df_apparts = pd.DataFrame(
        columns=
        ['id', 'source', 'source_is_partner', 'source_logo', 'source_label', 'search_type', 'owner_type',
         'rent', 'rent_max', 'area', 'room', 'bedroom', 'floor', 'type', 'buy_type', 'city', 'postal_code',
         'lat', 'lng',  'furnished', 'description', 'description_is_truncated', 'images', 'created_at',
         'expired_at', 'sendDate', 'previous_rent',  'previous_rent_at', 'favorite', 'nb_spam', 'contacted',
         'stops', 'features', 'new_real_estate', 'rentMinPerM2', 'clicked_at', 'webview_link', 'alert_id', 'page'
         ])

    #for counter, page in enumerate(range(1, nb_pages+1)):
    for page in trange(1, nb_pages+1):
        target_url = root_url + f'?filter=all&page={page}'
        r_apparts = session.get(target_url, headers=headers)
        df_temp = pd.DataFrame.from_records(data=r_apparts.json()['ads'])
        df_temp['page'] = page
        df_apparts = df_apparts.append(df_temp)
    #    logger.info(f'{counter+1} / {nb_pages} pages have been processed.')   
    return df_apparts


def get_all_apparts(df_alerts, session, headers):
    df_final = pd.DataFrame(
        columns=[
            'id', 'source', 'source_is_partner', 'source_logo', 'source_label',
            'search_type', 'owner_type', 'rent', 'rent_max', 'area', 'room', 'bedroom', 'floor', 'type', 'buy_type',
            'city', 'postal_code', 'lat', 'lng',  'furnished', 'description', 'description_is_truncated', 'images',
            'created_at', 'expired_at', 'sendDate', 'previous_rent',  'previous_rent_at', 'favorite', 'nb_spam',
            'contacted', 'stops', 'features', 'new_real_estate', 'rentMinPerM2', 'clicked_at', 'webview_link',
            'alert_id'
        ])
    for idx, alert in df_alerts.iterrows():
        logger.info(f'Starting the processing of the apparts of alert n°{idx + 1}')
        alert_id = alert['id']
        nb_pages = alert['nb_pages']
        df_alert = get_apparts(session, headers, alert_id, nb_pages)
        df_final = df_final.append(df_alert)
        logger.info(f'Finished processing the apparts of alert n°{idx + 1}')
    return df_final

