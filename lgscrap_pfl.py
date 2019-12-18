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
PROXIES = {'https': 'https://37.203.246.48:42911'}
MIN_TIME_SLEEP = 1
MAX_TIME_SLEEP = 3
MAX_COUNTS = 5
TIMEOUT = 10
URL_BASE = 'https://www.pfl-russia.com'

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
    symbols = ('абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ /-',
               'abvgdeejzijklmnoprstufhccss_yieuaABVGDEEJZIJKLMNOPRSTUFHCCSS_YIEUA___')
    tr = {ord(a):ord(b) for a, b in zip(*symbols)}
    return text.translate(tr)
def main():
    #python lgscrap_pfl.py https://37.203.246.48:42911 C:/data/leagues/_data
    proxies = str(sys.argv[1]) #https://37.203.246.48:42911
    print('got proxies: ', proxies)
    PROXIES = {'https': proxies}
    data_path = sys.argv[2] #C:/data/leagues/_data
    print('data path: ', data_path)
    
    CACHE_PATH = '{}/{}/'.format(data_path, 'pfl')
    if not os.path.exists(CACHE_PATH):
        os.makedirs(CACHE_PATH)
    CACHE_PATH_CLDS = '{}clds/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_CLDS):
        os.makedirs(CACHE_PATH_CLDS)
    CACHE_PATH_CLDS_RAW = '{}clds_raw/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_CLDS_RAW):
        os.makedirs(CACHE_PATH_CLDS_RAW)
    CACHE_PATH_CLDS_RFRS = '{}clds_rfrs/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_CLDS_RFRS):
        os.makedirs(CACHE_PATH_CLDS_RFRS)
    CACHE_PATH_CLDS_RFRS_RAW = '{}clds_rfrs_raw/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_CLDS_RFRS_RAW):
        os.makedirs(CACHE_PATH_CLDS_RFRS_RAW)
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
    CACHE_PATH_PLRS = '{}plrs/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_PLRS):
        os.makedirs(CACHE_PATH_PLRS)
    CACHE_PATH_PLRS_RAW = '{}plrs_raw/'.format(CACHE_PATH)
    if not os.path.exists(CACHE_PATH_PLRS_RAW):
        os.makedirs(CACHE_PATH_PLRS_RAW)
    
    #---CALENDAR---
    html = get_content(URL_BASE, TIMEOUT, PROXIES)
    soup = BeautifulSoup(html, 'lxml')
    urls_clds = {}
    for itm in soup.find_all('a', {'class': False, 'aria-haspopup': False}):
        if 'competitions/season' in itm['href']:
            url_ssn = URL_BASE + itm['href']
            html = get_content(url_ssn, TIMEOUT, PROXIES)
            soup_i = BeautifulSoup(html, 'lxml')
            for a in soup_i.find('div', {'class': "seasons-select"}).find_all('a'):
                urls_clds.update({
                    itm.text.strip() + ' '+ a.text.strip(): URL_BASE + a['href']
                })
    print(urls_clds)
    #---games by calendar---
    for cld_name, url_cld in urls_clds.items():
        print('processing: ', cld_name, ' | url: ', url_cld)
        html = get_content(url_cld, TIMEOUT, PROXIES)
        file_name = '{}calendar_{}.html'.format(CACHE_PATH_CLDS_RAW, translit(cld_name))
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(str(html))
        with open(file_name, 'r', encoding='utf-8') as file:
            html = file.read()
        soup = BeautifulSoup(html, 'lxml')
        date, tour, time = '', '', ''
        cld_games = []
        game = {}
        for itm in soup.find('table', {'class': "games-table"}).find_all('tr'):
            game = {}
            if itm.find_all('td', {'class': False, 'colspan': True}):
                date = itm.find('td', {'colspan': True}).text.strip()
            if itm.find_all('td', {'class': "games-date-tr", 'colspan': True}):
                tour = itm.text.strip()
            if itm.find('span', {'class': "match-date"}):
                time = itm.find('span', {'class': "match-date"}).text.strip()
            if len(itm.find_all('td')) > 2:
                game['date'] = date
                game['tour'] = tour
                game['time'] = time
                game['team-home'] = itm.find_all('td')[1].text.strip()
                if itm.find_all('td')[1].find('a'):
                    game['team-home_href'] = URL_BASE + itm.find_all('td')[1].find('a')['href']
                game['game-score'] = ' '.join(itm.find_all('td')[2].text.split())
                if itm.find_all('td')[2].find('a'):
                    game['game-score_href'] = URL_BASE + itm.find_all('td')[2].find('a')['href']
                game['team-away'] = itm.find_all('td')[3].text.strip()
                if itm.find_all('td')[3].find('a'):
                    game['team-away_href'] = URL_BASE + itm.find_all('td')[3].find('a')['href']
                cld_games.append(game)
        file_name = '{}calendar_{}.txt'.format(CACHE_PATH_CLDS, translit(cld_name))
        with open(file_name, 'w') as file:
            json.dump(cld_games, file)
        print('done: ', file_name)
    #---this part refers to referee set up for a tour of games---
    for cld_name, url_cld in urls_clds.items():
        ref_name = cld_name + '_referees'
        url_ref = url_cld.replace('calendar/', 'set-ref/')
        print('processing: ', ref_name, ' | url: ', url_ref)
        html = get_content(url_ref, TIMEOUT, PROXIES)
        file_name = '{}calendar_{}.html'.format(CACHE_PATH_CLDS_RFRS_RAW, translit(ref_name))
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(str(html))
        with open(file_name, 'r', encoding='utf-8') as file:
            html = file.read()
        soup = BeautifulSoup(html, 'lxml')
        date, tour, time = '', '', ''
        rfr_games = []
        game = {}
        for itm in soup.find('table', {'class': "games-table for-judges"}).find_all('tr'):
            game = {}
            if itm.find_all('td', {'class': False, 'colspan': "8"}):
                date = itm.find('td', {'colspan': True}).text.strip()
            if itm.find_all('td', {'class': "games-date-tr", 'colspan': True}):
                tour = itm.text.strip()
            if itm.find_all('td', {'class': "header"}):
                header = []
                for td in itm.find_all('td', {'class': "header"}):
                    header.append(td.text.strip())
            if len(itm.find_all('td')) > 6:
                game['date'] = date
                game['tour'] = tour
                game['header'] = header
                game['game_id'] = itm.find('span', {'class': "match-date"}).find('a').text.strip()
                game['game_id_href'] = itm.find('span', {'class': "match-date"}).find('a')['href']
                for itd, td in enumerate(itm.find_all('td', {'class': "jName"})):
                    if td.find('span', {'class': "jCity"}):
                        city = td.find('span', {'class': "jCity"}).text.strip()
                    else: 
                        city = ''
                    game['{}_{}'.format(itd, header[itd])] = {
                        'name': td.text.replace(city, '').strip(),
                        'city': city
                    }
                rfr_games.append(game)
        file_name = '{}calendar_{}.txt'.format(CACHE_PATH_CLDS_RFRS, translit(ref_name))
        with open(file_name, 'w') as file:
            json.dump(rfr_games, file)
        print('done: ', file_name)
    
    #---GAMES---
    games_urls = []
    teams_urls = []
    for file_name in os.listdir(CACHE_PATH_CLDS):
        with open('{}{}'.format(CACHE_PATH_CLDS, file_name), 'r') as file:
            data = json.load(file)
        for game in data:
            for k_game, v_game in game.items():
                if 'score_href' in k_game:
                    games_urls.append(v_game)
                elif '_href' in k_game:
                    teams_urls.append(v_game)
                else:
                    pass
    print('games total: ', len(games_urls), ' | unique games: ', len(set(games_urls)))
    print('teams total: ', len(teams_urls), ' | unique teams: ', len(set(teams_urls)))
    games_urls = sorted(list(set(games_urls)))
    teams_urls = sorted(list(set(teams_urls)))
    #---data by single game---
    start_index = len(os.listdir(CACHE_PATH_GMS))
    for game_url in games_urls[start_index:]:
        print('processing: ', game_url)
        html = get_content(game_url, TIMEOUT, PROXIES)
        file_name = '{}{}.html'.format(CACHE_PATH_GMS_RAW, game_url[game_url.find('/game') + 1 : -1].replace('-', '_'))
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(str(html))
        with open(file_name, 'r', encoding='utf-8') as file:
            html = file.read()
        soup = BeautifulSoup(html, 'lxml')
        game = {}
        #---overall info---
        if soup.find('div', {'class': "game-info"}).find('h3'):
            game['game_num'] = soup.find('div', {'class': "game-info"}).find('h3').text.strip()
        game['description'] = ' '.join([
            ' '.join(x.text.split()) for x in soup.find('div', {'class': "game-info"}).find_all('div', {'class': False})
        ])
        team_block = {}
        for team_num, team in enumerate(soup.find_all('div', {'class': "team-block"})):
            team_data = {}
            if team.find('a', {'class': "team-title"}):
                team_data['name'] = team.find('a', {'class': "team-title"}).text.strip()
                team_data['city'] = team.find('a', {'class': "team-city"}).text.strip()
            team_block.update({'team_' + str(team_num + 1): team_data})
        game['team_block'] = team_block
        if soup.find('div', {'class': "game-score-label"}):
            game['game_score'] = soup.find('div', {'class': "game-score-label"}).text.strip()
        #---text events---
        text_events = []
        header = []
        if soup.find('table', {'class': "text-events"}):
            for th in soup.find('table', {'class': "text-events"}).find_all('th'):
                header.append(th.text.strip())
            for tr in soup.find('table', {'class': "text-events"}).find('tbody').find_all('tr'):
                event = {}
                for itd, td in enumerate(tr.find_all('td')):
                    if td.find('div', {'class': True}):
                        event_text = td.find('div')['class']
                    else:
                        event_text = ' '.join(td.text.split())
                    event[header[itd]] = event_text
                    if td.find('a', {'href': True}):
                        event[header[itd] + '_href'] = URL_BASE + td.find('a', {'href': True})['href']
                text_events.append(event)
        game['text_events'] = text_events
        #---team players---
        teams = {}
        teams_lbl = ['teamA', 'teamB']
        players_lbl = ['main', 'reserve']
        if soup.find_all('div', {'class': "teams-tables"}):
            for lbl in teams_lbl:
                team = {}
                for imr, mr in enumerate(players_lbl):
                    players = []
                    header = []
                    team_table = soup.find_all('div', {'class': "teams-tables"})[imr]
                    for th in team_table.find('div', {'class': lbl}).find('thead').find_all('th'):
                        header.append(th.text.strip())
                    for tr in team_table.find('div', {'class': lbl}).find('tbody').find_all('tr'):
                        player = {}
                        for itd, td in enumerate(tr.find_all('td')):
                            player_text = ' '.join(td.text.split())
                            player[header[itd]] = player_text
                            if td.find('div', {'class': True}):
                                for iadd, add in enumerate(td.find_all('div', {'class': True})):
                                    player[header[itd] + '_' + str(iadd)] = add['class']
                            if td.find('a', {'href': True}):
                                player[header[itd] + '_href'] = URL_BASE + td.find('a', {'href': True})['href']
                        players.append(player)
                    team[mr] = players
                teams[lbl] = team
        game['teams'] = teams
        #---staff---
        staff = {}
        for lbl in teams_lbl:
            coaches = {}
            staff_team = []
            header = ['title', 'name']
            if soup.find('h3', {'class': "h3 gameH3"}):
                if 'Тренер' in soup.find('h3', {'class': "h3 gameH3"}).text:
                    if len(soup.find_all('div', {'class': "teams-tables"})) >= 3:
                        for tr in soup.find_all('div', {'class': "teams-tables"})[2].find('tbody').find_all('tr'):
                            staffer = {}
                            for itd, td in enumerate(tr.find_all('td')):
                                staffer.update({header[itd]: td.text.strip()})
                            staff_team.append(staffer)
                staff[lbl] = staff_team
        game['teams_staff'] = staff
        #---officials---
        officials = []
        header = ['title', 'name', 'city']
        if soup.find('table', {'class': "game-judjes table-datails"}):
            for tr in soup.find('table', {'class': "game-judjes table-datails"}).find('tbody').find_all('tr'):
                official = []
                for itd, td in enumerate(tr.find_all('td')):
                    official.append({header[itd]: td.text.strip()})
                officials.append(official)
        game['officials'] = officials
        #---save to file---
        file_name = '{}{}.txt'.format(CACHE_PATH_GMS, game_url[game_url.find('/game') + 1 : -1].replace('-', '_'))
        with open(file_name, 'w') as file:
            json.dump(game, file)
        print('done: ', file_name)

    #---TEAMS---
    start_index = len(os.listdir(CACHE_PATH_TMS))
    for team_url in teams_urls[start_index:]:
        print('processing: ', team_url)
        html = get_content(team_url, TIMEOUT, PROXIES)
        file_name = '{}team_{}.html'.format(
            CACHE_PATH_TMS_RAW, 
            team_url[team_url.find('/teams') + len('/teams') + 1 : -1].replace('-', '_').replace('/', '_')
        )
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(str(html))
        with open(file_name, 'r', encoding='utf-8') as file:
            html = file.read()
        soup = BeautifulSoup(html, 'lxml')
        team = {}
        #---overall info---
        if soup.find('h1', {'class': "club-header-name"}):
            team['club_name'] = soup.find('h1', {'class': "club-header-name"}).contents[0].strip()
            team['seasons'] = soup.find('h1', {'class': "club-header-name"}).find('div', {'class': "seasons-select"}).contents[0].strip()
            team['competitions'] = soup.find('h1', {'class': "club-header-name"}).find('a').text.strip()
            team['competitions_href'] = URL_BASE + soup.find('h1', {'class': "club-header-name"}).find('a')['href']
            for p in soup.find('div', {'class': "club-header-info"}).find_all('p'):
                if p.find('a'):
                    team[p.find('span').text.strip()] = p.find('a').text.strip()
                else:
                    team[p.find('span').text.strip()] = p.contents[1].strip()
        #---players---
        players = []
        if soup.find('div', {'class': "tab_team"}):
            for block in soup.find('div', {'class': "tab_team"}).find_all('div', {'class': "row article-grid-container main-players team-players"}):
                block_players = []
                for a in block.find_all('a', {'class': "article"}):
                    player = {}
                    block_players.append({'name': a.text.strip(), 'name_href': URL_BASE + a['href']})
                players.append({block.find('h2').text.strip(): block_players})
        team['players'] = players
        #---officials---
        if soup.find_all('table', {'class': "leadership"}):
            if len(soup.find_all('table', {'class': "leadership"})) == 2:
                officials = []
                headers = []
                for tr in soup.find_all('table', {'class': "leadership"})[0].find_all('tr'):
                    if tr.find_all('th'):
                        for th in tr.find_all('th'):
                            headers.append(th['class'][0] + '_' + th.text.strip() if th.text.strip() else th['class'][0])
                    if tr.find_all('td'):
                        official = {}
                        for itd, td in enumerate(tr.find_all('td')):
                            official[headers[itd]] = td.text.strip()
                        officials.append(official)
                team['officials'] = officials
                tbl_index = 1
            else:
                tbl_index = 0
        #----coaches and staff---
            coaches_and_staff = []
            headers = []
            for tr in soup.find_all('table', {'class': "leadership"})[tbl_index].find_all('tr'):
                if tr.find_all('th'):
                    for th in tr.find_all('th'):
                        headers.append(th['class'][0] + '_' + th.text.strip() if th.text.strip() else th['class'][0])
                if tr.find_all('td'):
                    person = {}
                    for itd, td in enumerate(tr.find_all('td')):
                        person[headers[itd]] = td.text.strip()
                    coaches_and_staff.append(person)
        team['coaches_and_staff'] = coaches_and_staff
        #---save to file---
        file_name = '{}team_{}.txt'.format(
            CACHE_PATH_TMS, 
            team_url[team_url.find('/teams') + len('/teams') + 1 : -1].replace('-', '_').replace('/', '_')
        )
        with open(file_name, 'w') as file:
            json.dump(team, file)
        print('done: ', file_name)
        
    #---PLAYERS---
    plrs_urls = []
    for file_name in os.listdir(CACHE_PATH_GMS):
        with open('{}{}'.format(CACHE_PATH_GMS, file_name), 'r') as file:
            data = json.load(file)
        for k1, v1 in data['teams'].items():
            for k2, v2 in v1.items():
                for item in v2:
                    for k3, v3 in item.items():
                        if '_href' in k3:
                            plrs_urls.append(v3)
    print('players total: ', len(plrs_urls), ' | unique players: ', len(set(plrs_urls)))
    plrs_urls = sorted(list(set(plrs_urls)))
    #---data by single player---
    start_index = len(os.listdir(CACHE_PATH_PLRS))
    for plr_url in plrs_urls[start_index:]:
        print('processing: ', plr_url)
        html = get_content(plr_url, TIMEOUT, PROXIES)
        file_name = '{}player_{}.html'.format(
            CACHE_PATH_PLRS_RAW, 
            plr_url[plr_url.find('/players') + len('/players') + 1 : -1].replace('-', '_').replace('/', '_')
        )
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(str(html))
        with open(file_name, 'r', encoding='utf-8') as file:
            html = file.read()
        soup = BeautifulSoup(html, 'lxml')
        player = {}
        #---overall info---
        overall_data = soup.find('h1', {'class': "club-header-name"})
        player['name'] = overall_data.contents[0].strip()
        if overall_data.find('div', {'aria-expanded': "false"}):
            player['season'] = overall_data.find('div', {'aria-expanded': "false"}).text.strip()
        else:
            player['season'] = overall_data.find('div', {'class': "seasons-select"}).contents[0].replace('Сезон ', '').strip()
        for p in overall_data.find_all('p'):
            if p.find('a'):
                player[p.span.text.strip()] = p.find('a').text.strip()
                player[p.span.text.strip() + '_href'] = URL_BASE + p.find('a')['href']
            else:
                player[p.span.text.strip()] = p.contents[1].strip()
        #---player stats---
        plr_stats = {}
        for stat_tbl in soup.find_all('div', {'class': "player-stat-by-team"}):
            plr_stats_team = []
            headers = []
            for th in stat_tbl.find('thead').find_all('th'):
                headers.append(th.text.strip())
            for tr in stat_tbl.find('tbody').find_all('tr'):
                row = {}
                for itd, td in enumerate(tr.find_all('td')):
                    row.update({headers[itd]: td.text.strip()})
                    if td.find('a'):
                        row.update({headers[itd] + '_href': URL_BASE + td.find('a')['href']})
                plr_stats_team.append(row)
            if stat_tbl.find('div', {'class': "h4"}):
                plr_stats[stat_tbl.find('div', {'class': "h4"}).text.strip()] = plr_stats_team
            else:
                plr_stats['default_team'] = plr_stats_team
        player['statistics'] = plr_stats
        #---save to file---
        file_name = '{}player_{}.txt'.format(
            CACHE_PATH_PLRS, 
            plr_url[plr_url.find('/players') + len('/players') + 1 : -1].replace('-', '_').replace('/', '_')
        )
        with open(file_name, 'w') as file:
            json.dump(player, file)
        print('done: ', file_name)

if __name__ == '__main__':
    main()        