import requests
import pickle
import re
from threading import Thread
from nintendeals import noa, noe
import pandas as pd

tg_token = '' # insert your telegram bot token here

endpoint = 'https://api.ec.nintendo.com/v1/price'

whishlist_src = [
    'The Elder Scrolls V: Skyrim',
    'ASTRAL CHAIN',
    'Bayonetta',
    'Bayonetta 2',
    'Darksiders Genesis',
    'Darksiders Warmastered Edition',
    'Darksiders II Deathinitive Edition',
    'Devil May Cry',
    'Devil May Cry 2',
    'Devil May Cry 3 Special Edition',
    'Spyro™ Reignited Trilogy',
    'Crash Bandicoot™ N. Sane Trilogy',
    'The Binding of Isaac: Afterbirth+',
    'Dead Cells',
    'Hades',
    'Cuphead',
    'BDSM: Big Drunk Satanic Massacre',
    'Hotline Miami Collection',
    'Risk of Rain 2',
    'Burnout™ Paradise Remastered',
    'FAST RMX',
    'Need for Speed™ Hot Pursuit Remastered',
    'BLAZBLUE CENTRALFICTION Special Edition',
    'NARUTO SHIPPUDEN™: Ultimate Ninja® STORM 4 ROAD TO BORUTO',
    '1-2-Switch',
    'ARMS',
    'Duck Game',
    'Mario Kart 8 Deluxe',
    'Super Mario Odyssey',
    'Super Mario Party',
    'Marvel Ultimate Alliance 3: The Black Order',
]

def prep_game_name(_game_name):
    game_regex = r'(\s*(™|®| HD$|\.$|$))'
    return re.sub(game_regex, '', _game_name)\
        .replace('–', '-').replace('—', '-').replace(' -', '-').replace('- ', ' ')\
        .replace('：', ':').replace(' :', ':').replace(': ', ' ').replace(':', ' ')\
        .replace('’', '\'').replace('‘', '\'').replace(' & ', ' and ').replace(' \'n\' ', '\'n\'')\
        .replace('  ', ' ')


whishlist = []
for game in whishlist_src:
    game_name = prep_game_name(game)
    if game_name.lower() not in [g.lower() for g in whishlist]:
        whishlist.append(game_name)

del whishlist_src

def get_prices(nsuids, country):

    params = {
        'ids': ','.join(nsuids),
    }

    shops = [
        {'country': 'RU', 'lang': 'ru'},
        {'country': 'US', 'lang': 'en'}
    ]

    for shop in shops:
        if shop['country'] == country:
            response = requests.get(endpoint, params={**shop, **params})
            response.encoding = 'utf8'
            return(response.json()['prices'])

    raise ValueError(f'Wrong country: "{country}"')


def get_games(region):
    for game in region.list_switch_games():
        yield {'title': prep_game_name(game.title), 'nsuid': game.nsuid}


def get_nsuids():
    res = {}

    def _task(region):
        res[region.__name__.replace('nintendeals.', '')] = [*get_games(region)]

    noa_task = Thread(target=_task, args=(noa,))
    noe_task = Thread(target=_task, args=(noe,))

    for t in (noa_task, noe_task):
        t.start()

    for t in (noa_task, noe_task):
        t.join()

    df_noa = pd.DataFrame(res['noa'])
    df_noe = pd.DataFrame(res['noe'])

    def _pk_ify(_game_name):
        return re.sub(r'\s', '', _game_name).lower()
    df_noa['temp_title'] = df_noa.title.apply(_pk_ify)
    df_noe['temp_title'] = df_noe.title.apply(_pk_ify)

    df_res = df_noa.merge(df_noe, how='outer', on='temp_title',
                          suffixes=('_noa', '_noe'))
    df_res['title'] = df_res.title_noa.fillna(df_res.title_noe)
    df_res.drop(['temp_title', 'title_noa', 'title_noe'], axis=1, inplace=True)

    return df_res.drop_duplicates()[['title', 'nsuid_noa', 'nsuid_noe']]


def load_nsuids():
    try:
        with open('nsuids.pkl', 'rb') as f:
            res = pickle.load(f)

        for game in whishlist:
            if game.lower() not in res.title.str.lower().tolist():
                raise Exception('Game not found')
    except:
        res = get_nsuids()

        with open('nsuids.pkl', 'wb') as f:
            pickle.dump(res, f)

        res.sort_values(by='title').to_excel('nsuids.xlsx', index=False)

        for game in whishlist:
            if game.lower() not in res.title.str.lower().tolist():
                raise Exception(f'Game not found: {game}')

    return res


def get_currency_rate(currency_code):
    request_url = f'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode={currency_code}&json'
    response = requests.get(request_url)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise ValueError(response.content)
    return response.json()[0]['rate']


def discounts_to_text(df):
    if df.shape[0] == 0:
        return None

    def _process(row):
        return (f"{row['title']}\n"
                'Is on discount for '
                f"{round(row['discount_price'],2)}₴ "
                f"(-{round(row['discount_pcn']*100)}%) "
                f"({row['currency_code']})")
    return '\n\n'.join(df.apply(_process, axis=1))


def send_message(text, **kwargs):
    data = {'text': text, 'chat_id': '-1001300378552', }
    data.update(kwargs)

    url = f'https://api.telegram.org/bot{tg_token}/sendMessage'

    response = requests.post(url, json=data)
    response.raise_for_status()

    return response.json()


if __name__ == '__main__':

    nsuids = load_nsuids()
    # nsuids.sort_values(by='title').to_excel('nsuids.xlsx', index=False)

    filter = nsuids.title.str.lower().isin(
        [game.lower() for game in whishlist])
    nsuids = nsuids[filter]

    prices_US = get_prices(
        nsuids.loc[nsuids.nsuid_noa.notna(), 'nsuid_noa'].tolist(), 'US')
    prices_RU = get_prices(
        nsuids.loc[nsuids.nsuid_noe.notna(), 'nsuid_noe'].tolist(), 'RU')

    nsuid_to_title = nsuids.melt(id_vars=['title'], value_vars=[
                                 'nsuid_noa', 'nsuid_noe'], value_name='nsuid')
    nsuid_to_title = nsuid_to_title.loc[nsuid_to_title.nsuid.notna(), [
        'nsuid', 'title']]
    nsuid_to_title = nsuid_to_title.drop_duplicates(
        subset='nsuid').set_index('nsuid').to_dict('index')
    nsuid_to_title = {key: val['title'] for key, val in nsuid_to_title.items()}

    offers = []

    for price in prices_RU+prices_US:
        nsuid = price['title_id']
        title = nsuid_to_title[str(nsuid)]
        if price.get('discount_price') is None:
            continue
        regular_price = float(price['regular_price']['raw_value'])
        discount_price = float(price['discount_price']['raw_value'])
        discount_price_start = price['discount_price']['start_datetime']
        discount_price_end = price['discount_price']['end_datetime']
        currency_code = price['regular_price']['currency']

        offers.append({
            'title': title,
            'regular_price': regular_price,
            'discount_price': discount_price,
            'currency_code': currency_code,
            'offer_start': discount_price_start,
            'offer_end': discount_price_end,
        })

    offers = pd.DataFrame(offers)
    offers['discount_amount'] = offers.regular_price - offers.discount_price
    offers['discount_pcn'] = offers.discount_amount.div(offers.regular_price)

    rates = {curr: get_currency_rate(
        curr) for curr in offers.currency_code.unique().tolist()}
    for currency, rate in rates.items():
        offers.loc[offers.currency_code == currency, 'regular_price'] = \
            offers.loc[offers.currency_code == currency, 'regular_price']*rate
        offers.loc[offers.currency_code == currency, 'discount_price'] = \
            offers.loc[offers.currency_code == currency, 'discount_price']*rate
        offers.loc[offers.currency_code == currency, 'discount_amount'] = \
            offers.loc[offers.currency_code ==
                       currency, 'discount_amount']*rate

    try:
        notified_prices = pd.read_pickle('processed.pkl')
    except:
        notified_prices = None

    if notified_prices is None:
        offers.to_pickle('processed.pkl')
    else:
        df_merge = offers.reset_index().merge(
            notified_prices.reset_index(),
            how='outer',
            on=['title', 'currency_code', 'offer_start', 'offer_end'],
            indicator=True
        )
        notified = df_merge.loc[df_merge._merge ==
                                'both', 'index_x'].values.tolist()
        if len(notified) > 0:
            offers.drop(notified, axis=0, inplace=True)
        left_off = df_merge.loc[df_merge._merge ==
                                'right_only', 'index_y'].values.tolist()
        if len(left_off) > 0:
            notified_prices.drop(left_off, axis=0, inplace=True)
        new = df_merge.loc[df_merge._merge ==
                           'left_only', 'index_x'].values.tolist()
        notified_prices = pd.concat([
            notified_prices,
            offers.loc[new, :]
        ])

        notified_prices.to_pickle('processed.pkl')

    to_notify = offers.sort_values(by=['discount_price'])\
        .drop_duplicates(subset='title', keep='first')

    message = discounts_to_text(to_notify)
    if message is not None:
        resp = send_message(message)