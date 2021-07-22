import json
import argparse
import os
import warnings
import csv

import PySimpleGUI as sg
import pandas as pd
from logzero import logger, logfile

from api_utils import authenticate, get_alerts, get_all_apparts, get_all_links, remove_expired
from processing_utils import features_engineering, cleaner, update_history_df, append_history_df, \
    metro_geo_pos_when_none

parser = argparse.ArgumentParser(description='Override the GUI if needed.')
# parser.add_argument('override', metavar='N', type=bool, nargs='+',
#                     help='an integer for the accumulator')
parser.add_argument('-e', '--email',
                    help='The Jinka account email address')
parser.add_argument('-p', '--password',
                    help='The Jinka account password')
parser.add_argument('-l', '--load', type=int, nargs='?',
                    help='Whether to load existing credentials: 0 or 1')
parser.add_argument('-s', '--save', type=int, nargs='?',
                    help='Whether to save the credentials. It requires the email and password parameters.')
parser.add_argument('-x', '--expired', nargs='?', const=1,
                    help='Whether to remove expired offers.')

args = parser.parse_args()

current_dir = os.getcwd()
path_list = current_dir.split(os.sep)

if path_list[-1] == 'src':
    current_dir = os.path.join(current_dir, '..')
    os.chdir(current_dir)

# Path to files 

CREDENTIALS_FILE = os.path.join(os.getcwd(), 'databases', 'credentials.json')
APPARTS_DB_PATH = os.path.join(os.getcwd(), 'databases', 'appart_links_db.json')
LAST_DELETED_PATH = os.path.join(os.getcwd(), 'databases', 'last_deleted_apparts.json')
HISTORY_PATH = os.path.join(os.getcwd(), 'data', 'history.csv')
APPARTS_CSV_PATH = os.path.join(os.getcwd(), 'data', 'apparts.csv')
APPARTS_XLSX_PATH = os.path.join(os.getcwd(), 'data', 'apparts.xlsx')
LOG_PATH = os.path.join(os.getcwd(), 'databases', 'logs.log')
DATABASES_PATH = os.path.join(os.getcwd(), 'databases')
DATA_PATH = os.path.join(os.getcwd(), 'data')

if os.path.exists(LOG_PATH):
    os.remove(LOG_PATH)

if not os.path.exists(DATABASES_PATH):
    os.mkdir(DATABASES_PATH)

if not os.path.exists(DATA_PATH):
    os.mkdir(DATA_PATH)

logfile(LOG_PATH)


def run_all(email, password, expired):
    s, headers = authenticate(email, password)

    if s == None:
        logger.critical('Aborting search, check your credentials.')
        quit()
    df_alerts = get_alerts(s, headers)
    df_apparts = get_all_apparts(df_alerts, s, headers)
    df_apparts = cleaner(df_apparts)
    df_apparts = features_engineering(df_apparts)

# Better features for df_aparts
    df_apparts = metro_geo_pos_when_none(df_apparts)



    df_history = append_history_df(df_apparts, HISTORY_PATH)

    df_apparts, new_expired_list = get_all_links(s, df_apparts, expired, APPARTS_DB_PATH)

    if expired:
        df_history = update_history_df(df_apparts, df_history, new_expired_list)
        df_apparts = remove_expired(s, df_apparts, new_expired_list, LAST_DELETED_PATH)

    # Transform df into csv_friendly format
    df_apparts = df_apparts.astype(str)
    df_apparts.to_csv(
        APPARTS_CSV_PATH,
        index=False,
        quotechar='"',
        quoting=csv.QUOTE_NONNUMERIC,
        encoding='utf-8')

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df_apparts.to_excel(APPARTS_XLSX_PATH, encoding='utf-8')

    df_history = df_history.astype(str)
    df_history.to_csv(HISTORY_PATH,
                      index=False,
                      quotechar='"',
                      quoting=csv.QUOTE_NONNUMERIC,
                      encoding='utf-8')


def create_main_window(credentials_file=CREDENTIALS_FILE):
    sg.theme()

    if os.path.exists(credentials_file):
        with open(credentials_file, 'r') as f:
            credentials = json.load(f)
        default_email = credentials['-EMAIL-']
        default_password = credentials['-PASSWORD-']
    else:
        default_email = ''
        default_password = ''

    def TextLabel(text, size):
        return sg.Text(text + ':', justification='r', size=size)

    layout = [[sg.T('Kajin')],
              [sg.T('We <3 scrapping')],
              [TextLabel('Login email', size=(25, 1)), sg.InputText(key='-EMAIL-', default_text=default_email)],
              [TextLabel('Password', size=(25, 1)),
               sg.InputText(key='-PASSWORD-', default_text=default_password, password_char='*')],
              [sg.Checkbox('Clean expired appartments', size=(10, 1), key='-EXPIRED-')],
              # [TextLabel('Theme', size=(25,1)), sg.Combo(sg.theme_list(), key='-THEME-', size=(20, 20), default_text=default_theme)],
              [sg.B('Run Application'), sg.B('Save credentials'), sg.B('Exit')]]

    return sg.Window('Main Application', layout, size=(500, 200))


if __name__ == '__main__':

    if (args.email == None) and (args.password == None) and (args.load == None) and (args.save == None) and (
            args.expired == None):
        window = None
        while True:
            if window == None:
                window = create_main_window()
                event, credentials = window.read()

            if event == 'Run Application':
                logger.info('Launching application')
                password = credentials['-PASSWORD-']
                email = credentials['-EMAIL-']
                expired = credentials['-EXPIRED-']
                window.close()
                run_all(email, password, expired=expired)
                break

            if event == 'Save credentials':
                with open(CREDENTIALS_FILE, 'w') as f:
                    json.dump(credentials, f)
                sg.Popup('Credentials saved', keep_on_top=True)
                window = None

            if event in (sg.WIN_CLOSED, 'Exit'):
                break
    else:
        if args.load == True:
            if os.path.exists(CREDENTIALS_FILE):
                with open(CREDENTIALS_FILE, 'r') as f:
                    credentials = json.load(f)
                email = credentials['-EMAIL-']
                password = credentials['-PASSWORD-']

        else:
            email = args.email
            password = args.password

        if args.save == True:
            credentials = {'-EMAIL-': email, '-PASSWORD-': password}
            with open(CREDENTIALS_FILE, 'w') as f:
                json.dump(credentials, f)

        run_all(email, password, expired=args.expired)
