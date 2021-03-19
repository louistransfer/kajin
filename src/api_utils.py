import pandas as pd
import requests
import os
from bs4 import BeautifulSoup
from tqdm import tqdm, trange
from logzero import logger

def authenticate(email, password):
    auth_url = 'https://api.jinka.fr/apiv2/user/auth'
    auth_dict = {'email':email, 'password':password}
    s = requests.Session()
    r_auth = s.post(auth_url, auth_dict)
    if r_auth.status_code == 200:
        logger.info('Authentification succeeded (200)')
        access_token = r_auth.json()['access_token']
    else:
        logger.critical(f'Authentification failed with error {r_auth.status_code}')
        return None, None

    headers = {
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
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
    df_alerts = pd.DataFrame(columns=['id', 'name', 'user_name', 'ads_per_day'])
    data_dict = {'id':[], 'name':[], 'user_name':[], 'ads_per_day':[], 'nb_pages':[], 'all':[], 'read':[],
    'unread':[], 'favorite':[], 'contact':[], 'deleted':[]}
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

def get_appart_response(session, alert_id, appart_id):

    alert_id = str(alert_id)
    appart_id = str(appart_id)
    headers = {
    'authority': 'api.jinka.fr',
    'upgrade-insecure-requests': '1',
    'dnt': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
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

    response = session.get('https://api.jinka.fr/alert_result_view_ad', headers=headers, params=params)
    return response

def expired_checker(response, source):

    is_expired = False
    if source in ['logic-immo', 'leboncoin', 'century21', 'meilleursagents', 'locservice', 'lagenceblue']:
        parsed_url = BeautifulSoup(response.text, 'html.parser')
    elif source in ['pap', 'seloger', 'paruvendu', 'laforet', 'orpi', 'avendrealouer']:
        parsed_url = response.url.split('/')
    else:
        return is_expired

    if source == 'logic-immo':
        item = parsed_url.find_all(class_="expiredTxt")
        if len(item)!=0:
            is_expired = True
    
    if source == 'pap':
        if parsed_url[3] == 'annonce':
            is_expired = True

    if source == 'seloger':
        if parsed_url[-1] == '#expiree':
            is_expired = True

    if source == 'leboncoin':
        item = parsed_url.find_all(class_="_1oejz _1hnil _1-TTU _35DXM")
        if len(item)!=0:
            is_expired = True

    if source == 'explorimmo':
        pass

    if source == 'paruvendu':
        if parsed_url[-1] == '#showError404':
            is_expired = True


    if source == 'century21':
        item = parsed_url.find_all(class_="content_msg")
        if len(item)!=0:
            if item[0].strong.text == "Nous sommes désolés, la page à laquelle vous tentez d'accéder n'existe pas.":
                is_expired = True

    if source == 'stephaneplaza':
        pass

    if source == 'meilleursagents':
        item = parsed_url.find_all(class_="error-page")
        if len(item)!=0:
            is_expired = True

    if source == 'flatlooker':
        pass

    if source == 'bienici':
        pass

    if source ==  'locservice':
        item = parsed_url.find_all(class_="louerecemment")
        if len(item)!=0:
            is_expired = True

    if source ==  'guyhoquet':
        pass

    if source == 'laforet':
        if parsed_url[3] == 'ville':
            is_expired = True

    if source == 'lagenceblue':
        item = parsed_url.find_all(class_="label label-warning")
        if len(item)!=0:
            is_expired = True
        

    if source == 'avendrealouer':
        if '#expiree' in parsed_url[-1]:
           is_expired = True 

    if source == 'orpi':
        if parsed_url[-2] == 'louer-appartement':
           is_expired = True 

    if source ==  'parisattitude':
        pass

    if source == 'fnaim':
        pass

    if source == 'erafrance':
        pass
    
    return is_expired

def get_all_links(session, df, expired, appart_db_path):

    
    if os.path.exists(appart_db_path) and (expired==False):
        logger.info('Found a preexisting links database.')
        df['link'] = None
        df['is_expired'] = False
        df_already_processed = pd.read_json(appart_db_path, orient='columns')

        unprocessed_index = set(df.index) - set(df_already_processed.index)
        processed_index = set(df.index).intersection(df_already_processed.index)
        df.loc[processed_index, 'link'] = df_already_processed['link']
        target_alert_ids = df.loc[unprocessed_index, 'alert_id'].tolist()
        apparts_source = df.loc[unprocessed_index, 'source'].tolist()

    else:
        if os.path.exists(appart_db_path)==False:
            logger.warn('No preexisting database has been found, generating a new one.')
        elif expired:
            logger.warn('Replacing the previous database in order to check for apparts expiration.')
        unprocessed_index = df.index
        df_already_processed = pd.DataFrame()
        target_alert_ids, unprocessed_index = df['alert_id'].tolist(), df.index
        apparts_source = df['source'].tolist()

    logger.info(f'{len(unprocessed_index)} new links have been detected.')

    if len(unprocessed_index)!=0:
        links = list(unprocessed_index.copy())
        expiration_list = list(unprocessed_index.copy())
        
        idx = 0
        for alert_id, appart_id, source in tqdm(zip(target_alert_ids, unprocessed_index, apparts_source), total=len(links)) :
            response = get_appart_response(session, alert_id, appart_id)
            is_expired = expired_checker(response, source)
            true_url = response.url
            links[idx] = true_url
            expiration_list[idx] = is_expired
            idx += 1

        df.loc[unprocessed_index, 'link'] = links
        df.loc[unprocessed_index, 'is_expired'] = expiration_list
        
        df_to_append = df.loc[unprocessed_index, ['link']]

        df_already_processed = df_already_processed.append(df_to_append)
        df_already_processed.to_json(appart_db_path, orient='columns')

        nb_expired = len(df[df['is_expired']==True])
        logger.warn(f'{nb_expired} appartments have expired.')

    return df

def remove_expired(session, df, last_deleted_path):

    df_expired = df[df['is_expired']]

    logger.info('Starting the cleaning of expired offers.')

    for appart_id, row in tqdm(df_expired.iterrows()):
        post_url = 'https://api.jinka.fr/apiv2/alert/' + row['alert_id'] + '/abuses'
        data = {'ad_id':appart_id, 'reason':'ad_link_404'}

        response = session.post(post_url, data=data)
    
    df_expired.to_json(last_deleted_path, orient='columns')
    cleaned_df = df[df['is_expired']==False]
    logger.info('Finished cleaning the expired appartments.')
    return cleaned_df


def get_apparts(session, headers, alert_id, nb_pages):
    root_url = 'https://api.jinka.fr/apiv2/alert/' + str(alert_id) + '/dashboard' 

    df_apparts = pd.DataFrame(columns= ['id', 'source', 'source_is_partner', 'source_logo', 'source_label', 'search_type', 'owner_type', 'rent', 'rent_max', 'area', 'room', 'bedroom', 'floor', 'type', 'buy_type', 'city', 'postal_code', 'lat', 'lng',  'furnished', 'description', 'description_is_truncated', 'images', 'created_at', 'expired_at', 'sendDate', 'previous_rent',  'previous_rent_at', 'favorite', 'nb_spam', 'contacted', 'stops', 'features', 'new_real_estate', 'rentMinPerM2', 'clicked_at', 'webview_link', 'alert_id', 'page'])

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
    df_final = pd.DataFrame(columns= ['id', 'source', 'source_is_partner', 'source_logo', 'source_label',
     'search_type', 'owner_type', 'rent', 'rent_max', 'area', 'room', 'bedroom', 'floor', 'type', 'buy_type',
      'city', 'postal_code', 'lat', 'lng',  'furnished', 'description', 'description_is_truncated', 'images',
       'created_at', 'expired_at', 'sendDate', 'previous_rent',  'previous_rent_at', 'favorite', 'nb_spam', 'contacted',
        'stops', 'features', 'new_real_estate', 'rentMinPerM2', 'clicked_at', 'webview_link', 'alert_id'])

    for idx, alert in df_alerts.iterrows():
        logger.info(f'Starting the processing of the apparts of alert n°{idx + 1}')
        alert_id = alert['id']
        nb_pages = alert['nb_pages']
        df_alert = get_apparts(session, headers, alert_id, nb_pages)
        df_final = df_final.append(df_alert)
        logger.info(f'Finished processing the apparts of alert n°{idx + 1}')
    return df_final

