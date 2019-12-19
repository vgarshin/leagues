import requests
import sys
import json
import os
import re
import datetime
import pickle
import socket
import base64
import pandas as pd
from time import sleep

MAX_COUNTS = 3
TIME_SLEEP = 1

def string2base64(s):
    return base64.b64encode(s.encode('utf-8')).decode('ascii')
def get_token_data(log_url, creds, data):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic {}'.format(string2base64(creds))
    }
    r = requests.post(
        log_url, 
        headers=headers,
        data=data
    )
    try:
        token_data = r.json()
    except:
        print(r.text, '\n')
        token_data = {}
    return token_data
def get_data(url, token, limit=100, offset=0, max_counts=5, time_sleep=5):
    offset = offset
    data = []
    count = 0
    while True:
        print('url: ', url, ' | processing offset from: ', offset)
        params = {'access_token': token, 'limit': limit, 'offset': offset}
        r = requests.get(url, params=params)
        if r.json()['success']:
            if not r.json()['errors']:
                if (len(r.json()['data']) == 0):
                    break
                elif isinstance(r.json()['data'], dict):
                    data = r.json()['data']
                    break
                else:
                    data.extend(r.json()['data'])
                    offset += len(r.json()['data'])
            else:
                print('errors: ', r.json()['errors'])
                count += 1
                sleep(time_sleep)
        else:
            print('success: ', r.json()['success'], ' | errors: ', r.json()['errors'])
            count += 1
            sleep(time_sleep)
        if count >= max_counts:
            break
    return data
def main():
    creds = str(sys.argv[1])
    print('got credentials: ', creds)
    DATA_PATH = '{}/rpl/'.format(sys.argv[2])
    print('data path: ', DATA_PATH)
    #---get token---
    log_url = 'https://rfpl.sportand.me/authserver/oauth/token'
    data = 'grant_type=client_credentials'
    token_data = get_token_data(log_url, creds, data)
    print(token_data)
    token = token_data['access_token']
    print('token: ', token)
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)
    #---BY PERSON---
    by_person_dict = {
        'clubofficialperson_data': 'https://rfpl.sportand.me/api/clubofficialperson',
        'officialperson_data': 'https://rfpl.sportand.me/api/officialperson',
        'person_data': 'https://rfpl.sportand.me/api/person',
        'player_data': 'https://rfpl.sportand.me/api/player',
        'teamrepresentative_data': 'https://rfpl.sportand.me/api/teamrepresentative'
    }
    for name, url in by_person_dict.items():
        print('processing: ', name)
        data = get_data(url, token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
        file_name = '{}{}.txt'.format(DATA_PATH, name)
        with open(file_name, 'w') as file:
            json.dump(data, file)
    #---BY CLUBS AND TEAMS---
    by_clubs_teams_dict = {
        'clubs': 'https://rfpl.sportand.me/api/club',
        'teams': 'https://rfpl.sportand.me/api/team'
    }
    for name, url in by_clubs_teams_dict.items():
        print('processing: ', name)
        data = get_data(url, token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
        file_name = '{}{}.txt'.format(DATA_PATH, name)
        with open(file_name, 'w') as file:
            json.dump(data, file)
    #---BY TOURNAMENTS---
    by_tournaments_dict = {
        'competitions': [
            'https://rfpl.sportand.me/api/competition', 
            'tournaments' #Идентификатор соревнования
        ], 
        'seasons': [
            'https://rfpl.sportand.me/api/season', 
            'applications', #Идентификатор сезона
            'tournaments' #Идентификатор сезона
        ], 
        'tournaments': [
            'https://rfpl.sportand.me/api/tournament', 
            'matches', #Идентификатор турнира
            'teams', #Идентификатор турнира
            'stages', #Идентификатор турнира
        ] 
    }
    for name, url_list in by_tournaments_dict.items():
        print('processing: ', name)
        data = get_data(url_list[0], token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
        file_name = '{}{}.txt'.format(DATA_PATH, name)
        with open(file_name, 'w') as file:
            json.dump(data, file)
        dir_name = '{}/{}'.format(DATA_PATH, name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        for item in data:
            id_item = item['id']
            for num_url_id in range(1, len(url_list)):
                print('processing: ', name, ' | sub: ', url_list[num_url_id], 'id: ', id_item)
                id_url = '{}/{}/{}'.format(url_list[0], id_item, url_list[num_url_id])
                id_data = get_data(id_url, token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
                file_name = '{}/id{}_{}.txt'.format(dir_name, id_item, url_list[num_url_id])
                with open(file_name, 'w') as file:
                    json.dump(id_data, file)
    id_tour_list = []
    dir_name = '{}/tournaments'.format(DATA_PATH, name)
    for file_name in [x for x in os.listdir(dir_name) if 'stages' in x]:
        with open('{}/{}'.format(dir_name, file_name), 'r') as file:
            data = json.load(file)
        id_tour_list.extend([x['id'] for x in data])
    print('total ids tours: ', len(id_tour_list), ' | unique ids: ', len(set(id_tour_list)))
    dir_name = '{}/{}'.format(DATA_PATH, 'tours')
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    url_tours = 'https://rfpl.sportand.me/api/tournamentstage'
    for id_tour in id_tour_list:
        id_url = '{}/{}'.format(url_tours, id_tour)
        id_data = get_data(id_url, token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
        file_name = '{}/id{}.txt'.format(dir_name, id_tour, url_list[num_url_id])
        with open(file_name, 'w') as file:
            json.dump(id_data, file)
    #---BY APPLICATIONS---
    by_applications_dict = {
        'applications': [
            'https://rfpl.sportand.me/api/application', 
            'coaches', #Идентификатор заявки клуба
            'heads', #Идентификатор заявки клуба
            'players' #Идентификатор заявки клуба
        ]
    }
    for name, url_list in by_applications_dict.items():
        print('processing: ', name)
        data = get_data(url_list[0], token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
        file_name = '{}{}.txt'.format(DATA_PATH, name)
        with open(file_name, 'w') as file:
            json.dump(data, file)
        dir_name = '{}/{}'.format(DATA_PATH, name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        for item in data:
            id_item = item['id']
            for num_url_id in range(1, len(url_list)):
                print('processing: ', name, ' | sub: ', url_list[num_url_id], 'id: ', id_item)
                id_url = '{}/{}/{}'.format(url_list[0], id_item, url_list[num_url_id])
                id_data = get_data(id_url, token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
                file_name = '{}/id{}_{}.txt'.format(dir_name, id_item, url_list[num_url_id])
                with open(file_name, 'w') as file:
                    json.dump(id_data, file)
    #---BY MATCHES---
    id_matches_list = []
    dir_name = '{}/tournaments'.format(DATA_PATH, name)
    for file_name in [x for x in os.listdir(dir_name) if 'matches' in x]:
        with open('{}/{}'.format(dir_name, file_name), 'r') as file:
            data = json.load(file)
        id_matches_list.extend([x['id'] for x in data])
    print('total ids matches: ', len(id_matches_list), ' | unique ids: ', len(set(id_matches_list)))
    dir_name = '{}/{}'.format(DATA_PATH, 'matches')
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    url_matches = 'https://rfpl.sportand.me/api/match'
    start_index = len(os.listdir(dir_name))
    print('start from: ', start_index)
    for id_match in id_matches_list[start_index:]:
        id_url = '{}/{}'.format(url_matches, id_match)
        id_data = get_data(id_url, token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
        file_name = '{}/id{}.txt'.format(dir_name, id_match)
        with open(file_name, 'w') as file:
            json.dump(id_data, file)
    #---DICTIONARIES---
    by_dictionaries_dict = {
        'countries': 'https://rfpl.sportand.me/api/country',
        'redcardtypes': 'https://rfpl.sportand.me/api/redcardtype',
        'refereecategories': 'https://rfpl.sportand.me/api/refereecategory',
        'regions': 'https://rfpl.sportand.me/api/region',
        'stadiums': 'https://rfpl.sportand.me/api/stadium',
        'yellowcardtypes': 'https://rfpl.sportand.me/api/yellowcardtype'
    }
    for name, url in by_dictionaries_dict.items():
        print('processing: ', name)
        data = get_data(url, token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
        file_name = '{}{}.txt'.format(DATA_PATH, name)
        with open(file_name, 'w') as file:
            json.dump(data, file)
    #---HISTORY---
    history_dict = {
        'history': 'https://rfpl.sportand.me/api/history'
    }
    for name, url in history_dict.items():
        print('processing: ', name)
        data = get_data(url, token, limit=1000, offset=0, max_counts=MAX_COUNTS, time_sleep=TIME_SLEEP)
        file_name = '{}{}.txt'.format(DATA_PATH, name)
        with open(file_name, 'w') as file:
            json.dump(data, file)
    print('all done')

if __name__ == '__main__':
    main()        