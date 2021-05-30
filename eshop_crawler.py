import requests
import pickle
import re
from threading import Thread
from nintendeals import noa, noe
import pandas as pd
import smtplib, ssl
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

smtp_server = os.environ.get("NOTIFICATION_SMTP_SERVER")
port = 587  # For starttls
sender_email = os.environ.get("NOTIFICATION_SENDER")
receiver_email = os.environ.get("NOTIFICATION_RECEIVER")
password = os.environ.get("NOTIFICATION_SMTP_PASSWORD")

whishlist_src = os.environ.get("GAMES").split(",")

endpoint = 'https://api.ec.nintendo.com/v1/price'

receiver_email = receiver_email.split(",")

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
        {'country': 'AT', 'lang': 'de'}
#        {'country': 'US', 'lang': 'en'}
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
        print('loading nsuid.pkl (rb)')
        with open('/data/nsuids.pkl', 'rb') as f:
            res = pickle.load(f)

        for game in whishlist:
            if game.lower() not in res.title.str.lower().tolist():
                raise Exception('Game not found')
    except:
        res = get_nsuids()

        print('dumping nsuid.pkl (wb)')
        with open('/data/nsuids.pkl', 'wb') as f:
            pickle.dump(res, f)

        res.sort_values(by='title')
        # res.sort_values(by='title').to_excel('nsuids.xlsx', index=False)

        for game in whishlist:
            if game.lower() not in res.title.str.lower().tolist():
                raise Exception(f'Game not found: {game}')

    return res

def discounts_to_text(df):
    if df.shape[0] == 0:
        return None

    def _process(row):
        return (f"{row['title']}\n"
                'Is on discount for '
                f"{round(row['discount_price'],2)} EUR "
                f"(-{round(row['discount_pcn']*100)}%) "
                f"({row['currency_code']})")
    return '\n\n'.join(df.apply(_process, axis=1))


def send_message(text):

    print(text)

    # Create a secure SSL context
    context = ssl.create_default_context()

    # Try to log in to server and send email
    try:
        server = smtplib.SMTP(smtp_server,port)
        server.ehlo() # Can be omitted
        server.starttls(context=context) # Secure the connection
        server.ehlo() # Can be omitted
        server.login(sender_email, password)

        for receiver in receiver_email:
            message = MIMEMultipart("alternative")
            message["Subject"] = "Nintendo Switch Deals Update"
            message["From"] = sender_email
            message["To"] = receiver

            message.attach(MIMEText(text, "plain"))

            res = server.sendmail(sender_email, receiver, message.as_string())

        return res
    except Exception as e:
        # Print any error messages to stdout
        print(e)
    finally:
        server.quit()


if __name__ == '__main__':

    nsuids = load_nsuids()
    # nsuids.sort_values(by='title').to_excel('nsuids.xlsx', index=False)

    filter = nsuids.title.str.lower().isin(
        [game.lower() for game in whishlist])
    nsuids = nsuids[filter]

#    prices_US = get_prices(
#        nsuids.loc[nsuids.nsuid_noa.notna(), 'nsuid_noa'].tolist(), 'US')
    prices_AT = get_prices(
        nsuids.loc[nsuids.nsuid_noe.notna(), 'nsuid_noe'].tolist(), 'AT')

    nsuid_to_title = nsuids.melt(id_vars=['title'], value_vars=[
                                 'nsuid_noa', 'nsuid_noe'], value_name='nsuid')
    nsuid_to_title = nsuid_to_title.loc[nsuid_to_title.nsuid.notna(), [
        'nsuid', 'title']]
    nsuid_to_title = nsuid_to_title.drop_duplicates(
        subset='nsuid').set_index('nsuid').to_dict('index')
    nsuid_to_title = {key: val['title'] for key, val in nsuid_to_title.items()}

    offers = []

    # +prices_US
    for price in prices_AT:
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

    if (len(offers) == 0):
        print('no offers at the moment.')
        exit()

    offers = pd.DataFrame(offers)
    offers['discount_amount'] = offers.regular_price - offers.discount_price
    offers['discount_pcn'] = offers.discount_amount.div(offers.regular_price)

    try:
        print('loading processed.pkl (read)')
        notified_prices = pd.read_pickle('/data/processed.pkl')
    except:
        notified_prices = None

    if notified_prices is None:
        print('writing to processed.pkl (to)')
        offers.to_pickle('/data/processed.pkl')
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

        print('writing to processed.pkl (to)')
        notified_prices.to_pickle('/data/processed.pkl')

    to_notify = offers.sort_values(by=['discount_price'])\
        .drop_duplicates(subset='title', keep='first')

    message = discounts_to_text(to_notify)
    if message is not None:
        resp = send_message(message)
    else:
        print('no message to send')