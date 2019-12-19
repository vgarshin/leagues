import os
import sys
import json
import re
import socket
import ssl
from urllib.request import Request, urlopen, URLError, HTTPError, ProxyHandler, build_opener, install_opener
from urllib.parse import quote, urlsplit, urlunsplit, urlencode
from random import randint
from time import sleep
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 YaBrowser/19.6.1.153 Yowser/2.5 Safari/537.36'
MIN_TIME_SLEEP = 1
MAX_TIME_SLEEP = 3
MAX_COUNTS = 5
TIMEOUT = 10
URL_BASE = 'https://1fnl.ru'

def get_content(url_page, timeout, proxies, file=False):
    counts = 0
    content = None
    while counts < MAX_COUNTS:
        try:
            request = Request(url_page)
            request.add_header('User-Agent', USER_AGENT)
            proxy_support = ProxyHandler(proxies)
            opener = build_opener(proxy_support)
            install_opener(opener)
            context = ssl._create_unverified_context()
            response = urlopen(request, context=context, timeout=timeout)
            if file:
                content = response.read()
            else:
                content =  response.read().decode(response.headers.get_content_charset())
            break
        except URLError as e:
            counts += 1
            print('URLError | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
        except HTTPError as e:
            counts += 1
            print('HTTPError | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
        except socket.timeout as e:
            counts += 1
            print('socket timeout | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
    return content
def translit(text):
    symbols = ('абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ /',
               'abvgdeejzijklmnoprstufhccss_yieuaABVGDEEJZIJKLMNOPRSTUFHCCSS_YIEUA__')
    tr = {ord(a):ord(b) for a, b in zip(*symbols)}
    return text.translate(tr)
def main():
    proxies = str(sys.argv[1])
    print('got proxies: ', proxies)
    PROXIES = {'https': proxies}
    data_path = sys.argv[2]
    print('data path: ', data_path)
    
    CACHE_PATH = '{}/{}/'.format(data_path, 'fnl')
    if not os.path.exists(CACHE_PATH):
        os.makedirs(CACHE_PATH)
    CACHE_PATH_CLDS = '{}_clds/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_CLDS):
        os.makedirs(CACHE_PATH_CLDS)
    CACHE_PATH_CLDS_RAW = '{}_clds_raw/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_CLDS_RAW):
        os.makedirs(CACHE_PATH_CLDS_RAW)
    CACHE_PATH_SSNS = '{}ssns/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_SSNS):
        os.makedirs(CACHE_PATH_SSNS)
    CACHE_PATH_SSNS_RAW = '{}ssns_raw/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_SSNS_RAW):
        os.makedirs(CACHE_PATH_SSNS_RAW)
    CACHE_PATH_GMS = '{}gms/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_GMS):
        os.makedirs(CACHE_PATH_GMS)
    CACHE_PATH_GMS_RAW = '{}gms_raw/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_GMS_RAW):
        os.makedirs(CACHE_PATH_GMS_RAW)
    CACHE_PATH_TMS = '{}tms/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_TMS):
        os.makedirs(CACHE_PATH_TMS)
    CACHE_PATH_TMS_RAW = '{}tms_raw/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_TMS_RAW):
        os.makedirs(CACHE_PATH_TMS_RAW)
    CACHE_PATH_TMS_SNS = '{}tms_sns/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_TMS_SNS):
        os.makedirs(CACHE_PATH_TMS_SNS)
    CACHE_PATH_TMS_RAW_SNS = '{}tms_raw_sns/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_TMS_RAW_SNS):
        os.makedirs(CACHE_PATH_TMS_RAW_SNS)
    CACHE_PATH_PLRS = '{}plrs/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_PLRS):
        os.makedirs(CACHE_PATH_PLRS)
    CACHE_PATH_PLRS_RAW = '{}plrs_raw/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_PLRS_RAW):
        os.makedirs(CACHE_PATH_PLRS_RAW)
        
    #---SEASONS---
    url_cld = '{}{}'.format(URL_BASE, '/champioship/results/')
    html = get_content(url_cld, TIMEOUT, PROXIES)
    file_name = '{}gms_calendar.html'.format(CACHE_PATH_CLDS_RAW)
    with open(file_name, 'w', encoding='utf-8') as file:
        file.write(str(html))
    with open(file_name, 'r', encoding='utf-8') as file:
        html = file.read()
    soup = BeautifulSoup(html, 'lxml')
    seasons_data = {}
    for item in soup.find_all('div', {'class': 'arhive-year'}):
        title_i = item.find('a')['title']
        href_i = item.find('a')['href']
        seasons_data[title_i] = '{}{}'.format(URL_BASE, href_i)
    print(seasons_data.keys())
    file_name = '{}gms_calendar.txt'.format(CACHE_PATH_CLDS)
    with open(file_name, 'w') as file:
        json.dump(seasons_data, file)
    print('data saved to: ', file_name)
    for ssn_name, url_ssn in seasons_data.items():
        ssn_name = translit(ssn_name)
        print('processing: ', ssn_name, ' | url: ', url_ssn)
        html = get_content(url_ssn, TIMEOUT, PROXIES)
        file_name = '{}season_{}.html'.format(CACHE_PATH_SSNS_RAW, ssn_name)
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(str(html))
        with open(file_name, 'r', encoding='utf-8') as file:
            html = file.read()
        soup = BeautifulSoup(html, 'lxml')
        tours = {}
        for table_games in soup.find('div', {'class': 'games_page'}).find_all('div', {'class': 'tizer'}):
            tour_games = []
            game = {}
            for row in table_games.find_all('tr'):
                if row['class'][0] == 'game-info':
                    if row.find('a'):
                        key = row.find('a')['title'].strip()
                        game[key] = row.find('a').text.strip()
                    if row.find('a', href=True):
                        game[key + '_href'] = URL_BASE + row.find('a')['href']
                if row['class'][0] == 'teams-info':
                    for col in row.find_all('td'):
                        key = col['class'][0]
                        game[key] = col.find('a', {'class': 'team-title'}).text.strip()
                        if col.find('div', {'class': 'game-score'}):
                            game[key + '_game-score'] = col.find('div', {'class': 'game-score'}).text.strip()
                        if col.find('a', {'class': 'team-title'}, href=True):
                            game[key + '_href'] = URL_BASE + col.find('a', {'class': 'team-title'})['href']
                    tour_games.append(game)
                    game = {}
            tours[table_games.find('span').text.strip()] = tour_games 
        file_name = '{}season_{}_tours_gms.txt'.format(CACHE_PATH_SSNS, ssn_name)
        with open(file_name, 'w') as file:
            json.dump(tours, file)
        print('done: ', ssn_name)
    #---GAMES---
    games_urls = []
    teams_urls = []
    for file_name in os.listdir(CACHE_PATH_SSNS):
        with open('{}{}'.format(CACHE_PATH_SSNS, file_name), 'r') as file:
            data = json.load(file)
        for tour, games in data.items():
            for game in games:
                for k_game, v_game in game.items():
                    if '_href' in k_game:
                        if 'results' in v_game:
                            games_urls.append(v_game)
                        elif ('teams' in v_game) and ('teams//' not in v_game):
                            teams_urls.append(v_game)
    print('games total: ', len(games_urls), ' | unique games: ', len(set(games_urls)))
    print('teams total: ', len(teams_urls), ' | unique teams: ', len(set(teams_urls)))
    games_urls = list(set(games_urls))
    teams_urls = list(set(teams_urls))
    #---callect data by game--- 
    start_index = len(os.listdir(CACHE_PATH_GMS))
    for game_url in games_urls[start_index:]:
        print('processing: ', game_url)
        html = get_content(game_url, TIMEOUT, PROXIES)
        file_name = '{}game_{}.html'.format(
            CACHE_PATH_GMS_RAW, 
            game_url[game_url.find('results') + len('results') + 1 : -1].replace('/', '_')
        )
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(str(html))
        with open(file_name, 'r', encoding='utf-8') as file:
            html = file.read()
        soup = BeautifulSoup(html, 'lxml')
        #---main game data---
        game = {}
        for td in soup.find('table', {'class': 'game-header'}).find_all('td'):
            team_data = {'team-title': td.find('a', {'class': 'team-title'}).text.strip()}
            if td.find('a', {'class': 'team-title'}, href=True):
                team_data['team-title' + '_href'] = URL_BASE + td.find('a', {'class': 'team-title'})['href']
            team_data['game-score'] = td.find('div', {'class': 'game-score'}).text.strip()
            game[td['class'][0]] = team_data
        #---teams---
        teams_players = {}
        if soup.find_all('div', {'class': 'teams-tables'}):
            for idx in [0, 1]: #0=main players, 1=reserve players
                tbl = soup.find_all('div', {'class': 'teams-tables'})[idx]
                for team in ['teamA', 'teamB']:
                    players = []
                    for tr in tbl.find('div', {'class': team}).find_all('tr'):
                        if tr.find_all('td'):
                            player = {}
                            player = {
                                'number': tr.find_all('td')[1].text.strip(),
                                'name': re.sub('[^а-яА-Яa-zA-Z]+', ' ', tr.find_all('td')[2].text).strip(),
                                'timepoints': re.sub('[^0-9]+', ' ', tr.find_all('td')[2].text).strip()
                            }
                            if tr.find_all('td')[2].find('a', href=True):
                                player['name' + '_href'] = URL_BASE + tr.find_all('td')[2].find('a')['href']
                            if tr.find_all('td')[2].find('a', {'class': not ''}):
                                player['type']= tr.find_all('td')[2].find('a')['class']
                                '''
                                <div class="LG">Легионер</div>
                                <div class="CG">Капитан команды</div>
                                <div class="YG">Молодой футболист</div>
                                '''
                            if tr.find_all('td')[2].find_all('div', {'class': not ''}):
                                player['timepoints_events'] = [(x['class'], x['title']) 
                                                               for x in tr.find_all('td')[2].find_all('div')]
                            players.append(player)
                        reserve_flag = '_reserve' if idx == 1 else ''
                        teams_players[team + reserve_flag] = players
            game['teams_players'] = teams_players
        #---game info---
        game_info = {}
        game_info['game_date'] = ' '.join(
            soup.find(
                'div', 
                {'class': "tab-pane active"}
            ).find(
                'div', 
                {'class': "game-date"}
            ).text.split()
        )
        if soup.find('div', {'class': 'tab-pane active'}).find('div', {'class': 'game-stadium'}):
            game_info['game_stadium'] = ' '.join(
                soup.find(
                    'div', 
                    {'class': 'tab-pane active'}
                ).find(
                    'div', 
                    {'class': 'game-stadium'}
                ).text.split()
            )
        game_officials = []
        if soup.find('div', {'class': "tab-pane active"}).find('table'):
            for tr in soup.find('div', {'class': "tab-pane active"}).find('table').find_all('tr'):
                game_officials.append(
                    {tr.find_all('td')[0].text.strip(): tr.find_all('td')[1].text.strip()}
                )
        elif soup.find('div', {'class': "tab-pane active"}).find('div', {'class': 'game-information'}):
            for p in soup.find('div', {'class': "tab-pane active"}).find('div', {'class': 'game-information'}).find_all('p'):
                if p.find('strong'):
                    game_officials.append(
                        {p.find('strong').text.strip(): ' '.join(p.text.split()).strip()}
                    )
                elif p.text == '':
                    pass
                else:
                    game_info['game_stadium'] = p.text.strip()

        else:
            pass
        game_info['game_officials'] = game_officials
        game['game_info'] = game_info
        #---game timeline---
        timeline = []
        if soup.find('table', {'class': 'text-events'}):
            for tr in soup.find('table', {'class': 'text-events'}).find_all('tr'):
                event = {}
                for td in tr.find_all('td'):
                    event[td['class'][0]] = ' '.join(td.text.split()).strip()
                    if td.find_all('a', href=True):
                        for i in range(len(td.find_all('a', href=True))):
                            event[td['class'][0] + '_href_' + str(i)] = URL_BASE + td.find_all('a')[i]['href']
                    if td.find_all('a', {'class': not ''}):
                        for i in range(len(td.find_all('a', {'class': not ''}))):
                            event[td['class'][0] + '_type_' + str(i)] = td.find_all('a', {'class': not ''})[i]['class']
                    if td.find_all('div', {'class': not ''}):
                        event[td['class'][0] + '_details'] = [x['class'] for x in td.find_all('div')]
                if event:
                    timeline.append(event)
        game['timeline'] = timeline
        #---save game data to json txt file---
        file_name = '{}game_{}.txt'.format(
            CACHE_PATH_GMS, 
            game_url[game_url.find('results') + len('results') + 1 : -1].replace('/', '_')
        )
        with open(file_name, 'w') as file:
            json.dump(game, file)
        print('done: ', file_name)
    #---TEAMS---
    start_index = len(os.listdir(CACHE_PATH_TMS))
    for team_url in teams_urls[max(0, start_index - 1):]:
        print('processing: ', team_url)
        html = get_content(team_url, TIMEOUT, PROXIES)
        soup = BeautifulSoup(html, 'lxml')
        #---get urls for all seasons---
        if soup.find('li', {'class': 'btn btn-primary current'}):
            team_url_seasons = {
                soup.find('li', {'class': 'btn btn-primary current'}).string.strip(): team_url
            }
            for a in soup.find('div', {'class': 'team-seasons'}).find_all('a'):
                team_url_seasons.update({a.text.strip(): team_url + a['href']})
        else:
            team_url_seasons = {'no_seasons': team_url}   
        print('total seasons: ', len(team_url_seasons))
        #---get data by seasons---
        for ssn, team_url_ssn in team_url_seasons.items():
            print('processing: ', ssn, ' | url: ', team_url_ssn)
            html = get_content(team_url_ssn, TIMEOUT, PROXIES)
            soup = BeautifulSoup(html, 'lxml')
            if 'showseason' in team_url_ssn:
                file_name = '{}team_{}_ssn_{}.html'.format(
                    CACHE_PATH_TMS_RAW_SNS, 
                    team_url[team_url.find('teams') + len('teams') + 1 : -1].replace('/', '_'), 
                    str(ssn).replace('/', '_')
                )
            else:
                file_name = '{}team_{}_ssn_{}.html'.format(
                    CACHE_PATH_TMS_RAW, 
                    team_url[team_url.find('teams') + len('teams') + 1 : -1].replace('/', '_'), 
                    str(ssn).replace('/', '_')
                )
            with open(file_name, 'w', encoding='utf-8') as file:
                file.write(str(html))
            with open(file_name, 'r', encoding='utf-8') as file:
                html = file.read()
            soup = BeautifulSoup(html, 'lxml')
            #===TEAM DATA COLLECT===
            team = {}
            #---overall description---
            team_description = {}
            if soup.find('div', {'class': "tab-pane", 'id': "team-description"}):
                for p in soup.find('div', {'class': "tab-pane", 'id': "team-description"}).find_all('p'):
                    key = ''.join([' '.join(x.text.replace(':', '').strip().split()) for x in p.find_all('strong')])
                    value = ' '.join(p.text.split()).replace(key, '').replace(':', '').strip()
                    team_description[key] = value
            team['team_description'] = team_description
            #---team players and officials---
            players = []
            officials = {}
            if soup.find('div', {'class': "tab_team"}):
                if soup.find('div', {'class': "tab_team"}).find_all('li'):
                    #---this works for new seasons---
                    for ctg in soup.find('div', {'class': "tab_team"}).find_all('li'):
                        player = {}
                        player['name'] = ctg.find('a', {'class': 'name'}).text.strip()
                        player['href'] = URL_BASE + ctg.find('a', {'class': 'name'})['href']
                        player['num'] = ctg.find('div', {'class': 'num'}).text.strip()
                        if len(ctg.find('a', {'class': 'name'})['class']) >= 2:
                            player['type'] = [x for x in ctg.find('a', {'class': 'name'})['class'][1:]]
                        players.append(player)
                else:
                    #---this works for old seasons---
                    for tr in soup.find('div', {'class': "tab_team"}).find_all('tr'):
                        player = {}
                        if tr.find('td', {'class': 'name'}):
                            if tr.find('td', {'class': 'num'}):
                                player['num'] = tr.find('td', {'class': 'num'}).text.strip()
                                player['name'] = tr.find('td', {'class': 'name'}).text.strip()
                                player['href'] = URL_BASE + tr.find('td', {'class': 'name'}).find('a')['href']
                                player['birthday'] = tr.find('td', {'class': 'birthday'}).text.strip()
                                player['citizenship'] = tr.find('td', {'class': 'citizenship'}).text.strip()
                                player['dates'] = tr.find('td', {'class': 'dates'}).text.strip()
                                if len(tr.find('td', {'class': 'name'})['class']) >= 2:
                                    player['type'] = [x for x in tr.find('td', {'class': 'name'})['class'][1:]]
                                players.append(player)
                            else:
                                officials[
                                    tr.find('td', {'class': 'post'}).text.strip()
                                ] = tr.find('td', {'class': 'name'}).text.strip()
                team['players'] = players
                #---officials data exists only for new seasons---
                for tbl in soup.find('div', {'class': "tab_team"}).find_all('table', {'class': "leadership"}):
                    officials_part = []
                    for tr in tbl.find_all('tr'):
                        if tr.find_all('td'):
                            officials_part.append(
                                {tr.find_all('td')[1].string.strip(): tr.find_all('td')[0].string.strip()}
                            )
                    officials[tbl.th.string.strip()] = officials_part
            team['officials'] = officials
            #---team stats---
            header_1 = []
            header_2 = []
            stat_table = {}
            if soup.find('div', {'class': "tab-pane", 'id': "team-stat"}):
                for th in soup.find('div', {'class': "tab-pane", 'id': "team-stat"}).find('thead').find_all('th', {'colspan': 2}):
                    header_1.extend([th.text] * 2)
                for th in soup.find('div', {'class': "tab-pane", 'id': "team-stat"}).find('thead').find_all('th', {'colspan': False}):
                    header_2.append(th.text)
                header = [h1 + '_' + h2 for h1, h2 in zip(header_1, header_2[1:])]
                for tr in soup.find('div', {'class': "tab-pane", 'id': "team-stat"}).find('tbody').find_all('tr'):
                    row = {}
                    if tr.find_all('td', {'colspan': 2}):
                        for i_td, td in enumerate(tr.find_all('td', {'class': False})):
                            row[header[i_td * 2]] = td.string.strip()
                    else:
                        for i_td, td in enumerate(tr.find_all('td', {'class': False})):
                            row[header[i_td]] = td.string.strip()
                    stat_table[tr.find('td', {'class': "table_title"}).string.strip()] = row
            team['team_stats'] = stat_table
            #===WRITE TO JSON FILE===
            if 'showseason' in team_url_ssn:
                file_name = '{}team_{}_ssn_{}.txt'.format(
                    CACHE_PATH_TMS_SNS, 
                    team_url[team_url.find('teams') + len('teams') + 1 : -1].replace('/', '_'), 
                    str(ssn).replace('/', '_')
                )
            else:
                file_name = '{}team_{}_ssn_{}.txt'.format(
                    CACHE_PATH_TMS, 
                    team_url[team_url.find('teams') + len('teams') + 1 : -1].replace('/', '_'), 
                    str(ssn).replace('/', '_')
                )
            with open(file_name, 'w') as file:
                json.dump(team, file)
            print('done: ', file_name)
    #---PLAYERS---
    def get_players_url(path):
        players_urls = []
        for file_name in os.listdir(path):
            with open('{}{}'.format(path, file_name), 'r') as file:
                data = json.load(file)
            if 'players' in data.keys():
                for player in data['players']:
                    for k_plr, v_plr in player.items():
                        if 'href' in k_plr:
                            players_urls.append(v_plr)
        return players_urls
    players_urls = get_players_url(CACHE_PATH_TMS)
    players_urls.extend(get_players_url(CACHE_PATH_TMS_SNS))
    print('players total: ', len(players_urls), ' | unique players: ', len(set(players_urls)))
    players_urls = list(set(players_urls))
    #---collect data by player---
    start_index = len(os.listdir(CACHE_PATH_PLRS))
    for plr_url in players_urls[max(0, start_index - 1):]:
        print('processing: ', plr_url)
        #---save raw html---
        html = get_content(plr_url, TIMEOUT, PROXIES)
        file_name = '{}player_{}.html'.format(
            CACHE_PATH_PLRS_RAW, 
            plr_url[plr_url.find('players/') + len('players/') : -1].replace('/', '')
        )
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(str(html))
        with open(file_name, 'r', encoding='utf-8') as file:
            html = file.read()
        soup = BeautifulSoup(html, 'lxml')
        #---parce data on player---
        seasons = {}
        if soup.find('select', {'id': "js-player-season"}):
            for op in soup.find('select', {'id': "js-player-season"}).find_all('option'):
                seasons[op['value']] = op.text.strip()
        player = {}
        player['seasons'] = seasons
        player['name'] = soup.find('div', {'class': 'tizer-header'}).find('span').text.strip()
        if soup.find('div', {'class': 'player-info-block'}):
            for ssn in soup.find('div', {'class': 'player-info-block'}).find_all('div', {'class': 'season-block'}):
                player_ssn = {}
                for tr in ssn.find('tbody').find_all('tr'):
                    key = tr.find_all('td')[0].text.strip()
                    value = tr.find_all('td')[1].text.strip()
                    player_ssn[key] = value
                    if tr.find_all('td')[1].find('a'):
                        key = key + '_href'
                        value = URL_BASE + tr.find_all('td')[1].find('a')['href']
                        player_ssn[key] = value
                key_season = ssn['class'][1][len('js-season-'):]
                player['season_' + seasons[key_season]] = player_ssn
        if soup.find('div', {'class': 'player-stat'}):
            for ssn in soup.find('div', {'class': 'player-stat'}).find_all('div', {'class': 'season-block'}):
                stat_ssn = []
                headers = []
                if ssn.find('thead'):
                    for th in ssn.find('thead').find_all('th'):
                        if th.find('a'):
                            headers.append(th.find('a')['title'].strip())
                        else:
                            headers.append(th.text.strip())
                    for tr in ssn.find('tbody').find_all('tr'):
                        stat_ssn_game = {}
                        for hd, td in zip(headers, tr.find_all('td')):
                            stat_ssn_game[hd] = ' '.join(td.text.split()).strip()
                        stat_ssn.append(stat_ssn_game)
                key_season = ssn['class'][1][len('js-season-'):]
                player['season_stat_' + seasons[key_season]] = stat_ssn
        #---save player data to json---
        file_name = '{}player_{}.txt'.format(
            CACHE_PATH_PLRS, 
            plr_url[plr_url.find('players/') + len('players/') : -1].replace('/', '')
        )
        with open(file_name, 'w') as file:
            json.dump(player, file)
        print('done: ', file_name)
    print('all done')

if __name__ == '__main__':
    main()