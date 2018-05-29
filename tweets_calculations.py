import os
import sqlite3
import json
import re
from string import ascii_letters, whitespace
import html
from datetime import datetime

DB_NAME = 'db.sqlite3'

PATH = os.path.dirname(os.path.abspath(__file__))

TWEETS_FILE = os.path.join(PATH,'three_minutes_tweets.json.txt')
AFINN_DICT_FILE = os.path.join(PATH,'AFINN-111.txt')

good_chars = (ascii_letters + whitespace).encode()
junk_chars = bytearray(set(range(0x100)) - set(good_chars))

conn = sqlite3.connect(DB_NAME)


def drop_table():
    """
        drop table
    """

    print("dropping table ...")
    cursor = conn.cursor()
    tables = ['tweet', 'user', 'media', 'tweet_old']

    try:
        for table in tables:
            cursor.execute("DROP TABLE IF EXISTS {};".format(table))
        conn.commit()
        print("table dropped\n")
    except Exception as msg:
        print("Command skipped:", msg)


def create_table():
    """
        creating table
    """

    cursor = conn.cursor()
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS tweet(
                            tweet_id INTEGER PRIMARY_KEY,
                            user_id INTEGER,
                            name TEXT,
                            tweet_text TEXT,
                            country_code TEXT,
                            display_url TEXT,
                            lang TEXT,
                            created_at TIMESTAMP,
                            location TEXT);""")
        conn.commit()
    except Exception as msg:
        print("Command skipped: ", msg)
    print("table Tweet created\n")


def to_datetime(dt):
    return datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')


def insert_one_row(row):
    """
        insert one row
    """

    try:
        cursor = conn.cursor()
        query = """INSERT INTO Tweet(
                   tweet_id, user_id, name, tweet_text, country_code, display_url, lang, created_at, location)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        cursor.execute(query, row)
    except Exception as msg:
        print("Command skipped: ", msg)


def delete_all_data():
    """
        СѓРґР°Р»РµРЅРёРµ РІСЃРµС… РґР°РЅРЅС‹С…
    """

    print("deleting all rows ...")
    try:
        cursor = conn.cursor()
        query = """DELETE FROM Tweet"""
        cursor.execute(query)
    except sqlite3.IntegrityError as error:
        print("Error", error)
    conn.commit()
    print("all rows deleted\n")


def clean_text(text):
    """
        РѕС‡РјСЃС‚РєР° С‚РµРєСЃС‚Р° РїСЂРё Р°РЅР°Р»РёР·Рµ РЅР° РІС…РѕРґРµРЅРёРµ СЃР»РѕРІР° РІ СЃР»РѕРІР°СЂСЊ AFINN
    """
    return text.encode('utf-8', 'ignore').translate(None, junk_chars).decode().strip().lower()


def clean_column(data):
    """
        РѕС‡РјСЃС‚РєР° РґР°РЅРЅС‹С… РїСЂРё РІСЃС‚Р°РІРєРµ РІ Р‘Р”
    """
    return html.escape(data.strip())


def clean_word(word):
    """
        РѕС‡РјСЃС‚РєР° СЃР»РѕРІР° РїСЂРё Р°РЅР°Р»РёР·Рµ РЅР° РІС…РѕРґРµРЅРёРµ СЃР»РѕРІР° РІ СЃР»РѕРІР°СЂСЊ AFINN
    """
    return word.strip().lower()


def load_tweet(file_name):
    """
        С‡С‚РµРЅРёРµ С‚РІРёС‚РѕРІ РёР· С„Р°Р№Р»Р° Рё Р·Р°РїРёСЃСЊ РёС… РІ Р‘Р”
    """

    print("loading tweets from {} file ...".format(file_name))
    with open(file_name, 'r') as file:
        row_counter = 0

        # РїРѕР»СѓС‡РµРЅРёРµ РґР°РЅРЅС‹С… РёР· json Рё РїСЂРёСЃРІРѕРµРЅРёРµ РїРµСЂРµРјРµРЅРЅС‹Рј
        line = file.readline()
        while line:
            tweet = json.loads(line)

            created_at = tweet.get('created_at')
            # Р·Р°РіСЂСѓР·РєР° С‚РѕР»СЊРєРѕ СЃРѕР·РґР°РЅРЅС‹С… С‚РІРёС‚РѕРІ
            if created_at:
                tweet_id = tweet.get('id')
                user = tweet.get('user')
                user_id = user.get('id')
                name = user.get('name')
                tweet_text = tweet.get('text')
                place = tweet.get('place')
                country_code = place.get('country_code') if tweet.get('place') else ''
                lang = user.get('lang')
                location = user.get('location')
                media = tweet.get('entities').get('media') if tweet.get('entities') else ''
                display_url = ''
                # РІСЃС‚Р°РІРєР° РјРµРґРёР° СЂРµСЃСѓСЂСЃРѕРІ, РёС… РјРѕР¶РµС‚ Р±С‹С‚СЊ РјРЅРѕРіРѕ
                if media:
                    for item in range(len(media)):
                        display_url = media[item].get('display_url')
                        row = (tweet_id, user_id, clean_column(name), clean_column(tweet_text), \
                               clean_column(country_code), clean_column(display_url), clean_column(lang), \
                               to_datetime(created_at), clean_column(location))

                        # РІСЃС‚Р°РІРєР° Р·Р°РїРёСЃРё
                        insert_one_row(row)
                        row_counter += 1
                else:
                    row = (tweet_id, user_id, clean_column(name), clean_column(tweet_text), clean_column(country_code),\
                           clean_column(display_url), clean_column(lang), to_datetime(created_at), clean_column(location))

                    # РІСЃС‚Р°РІРєР° Р·Р°РїРёСЃРё
                    insert_one_row(row)
                    row_counter += 1

            line = file.readline()
    conn.commit()
    print("file with tweets was read with {} rows\n".format(row_counter))


def add_column_sentiment():
    """
        РґРѕР±Р°РІР»РµРЅРёРµ РєРѕР»РѕРЅРєРё 'tweet_sentiment' РІ С‚Р°Р±Р»РёС†Сѓ 'Tweet'
    """

    print("adding tweet_sentiment columns ...")
    cursor = conn.cursor()
    try:
        cursor.execute("""ALTER TABLE tweet ADD tweet_sentiment INTEGER DEFAULT 0;""")
        conn.commit()
    except Exception as msg:
        print("Command skipped: ", msg)
    print("columns tweet_sentiment added\n")


def select_data(query=None):
    """
        Р·Р°РїСЂРѕСЃ РІСЃРµС… Р·Р°РїРёСЃРµР№ С‚Р°Р±Р»РёС†С‹ СЃ С‚РІРёС‚Р°РјРё Р»РёР±Рѕ РїРёС€РµРј СЃРІРѕР№ Р·Р°РїСЂРѕСЃ РІ query
    """

    cursor = conn.cursor()
    try:
        if query is None:
            for row in cursor.execute("""select * from tweet;"""):
                print(row)
        else:
            for row in cursor.execute("""{};""".format(query)):
                print(row)
    except Exception as msg:
        print("Command skipped: ", msg)


def get_afinn_dict(file_name):
    """
        Р·Р°РіСЂСѓР·РєР° AFINN СЃР»РѕРІР°СЂСЏ
    """

    print("loading {} file ...".format(file_name))
    dic = {}
    with open(file_name, 'r') as file:
        for data in file.readlines():
            word, value = data.split('\t')
            dic[word] = int(value)

    print("AFINN file was read with {} rows".format(len(dic)))
    return dic


def calculate_tweet_sentiment(variant=2):
    """
        СЂР°СЃС‡РµС‚ Р·РЅР°С‡РµРЅРёР№ tweet_sentiment
        variant=1 С‚РІРёС‚ Р±СЉРµС‚СЃСЏ РЅР° СЃР»РѕРІР° Рё СЃРјРѕС‚СЂСЏС‚СЃСЏ РІ afinn dict (Р±РѕР»РµРµ Р±С‹СЃС‚СЂС‹Р№)
        variant=2 РїСЂРѕС…РѕРґ РїРѕ afinn dict Рё СЃРјРѕС‚СЂСЏС‚СЃСЏ РЅР° РІС…РѕР¶РґРµРЅРёРµ РІ С‚РІРёС‚Рµ (РјРµРЅРµРµ Р±С‹СЃС‚СЂС‹Р№)
    """

    print("calculating tweet_sentiment ...")

    # Р·Р°РіСЂСѓР¶Р°РµРј afinn dict
    afinn_data = get_afinn_dict(file_name=AFINN_DICT_FILE)

    cursor = conn.cursor()
    sentiment_dict = {}

    # РїСЂРѕС…РѕРґРёРј РїРѕ СЃР»РѕРІР°СЂСЋ Рё СЃРјРѕС‚СЂРёРј РІС…РѕР¶РґРµРЅРёРµ СЃР»РѕРІ РІ С‚РІРёС‚Рµ
    # СЃРґРµР»Р°РЅРѕ 2 РІР°СЂРёР°РЅС‚Р°
    # try:
    for rid, tweet_text in cursor.execute("select tweet_id, tweet_text from tweet;"):
        tweet_text = clean_text(tweet_text)
        if variant == 1:
            # С‚РІРёС‚ Р±СЉРµС‚СЃСЏ РЅР° СЃР»РѕРІР° Рё СЃРјРѕС‚СЂСЏС‚СЃСЏ РІ afinn dict (Р±РѕР»РµРµ Р±С‹СЃС‚СЂС‹Р№, РјРµРЅРµРµ РєР°С‡РµСЃС‚РІРµРЅРЅС‹Р№)
            for word in clean_text(tweet_text).split(' '):
                word = clean_word(word)
                if word in afinn_data:
                    sentiment_dict[rid] = sentiment_dict.get(rid, [])+[afinn_data.get(word)]
        else:

            # РїСЂРѕС…РѕРґ РїРѕ afinn dict Рё СЃРјРѕС‚СЂСЏС‚СЃСЏ РЅР° РІС…РѕР¶РґРµРЅРёРµ РІ С‚РІРёС‚Рµ (РјРµРЅРµРµ Р±С‹СЃС‚СЂС‹Р№, Р±РѕР»РµРµ РєР°С‡РµСЃС‚РІРµРЅРЅС‹Р№)
            for word, val in afinn_data.items():
                if word in tweet_text:
                    sentiment_dict[rid] = sentiment_dict.get(rid, [])+[afinn_data.get(word)]
    # except Exception as msg:
    #     print("Command skipped: ", msg)

    # СЂР°СЃС‡РёС‚С‹РІР°РµРј СЃСЂРµРґРЅРёР№ sentiment
    for rid, values in sentiment_dict.items():
        sentiment_dict[rid] = round(sum(values)/len(values))

    print("tweet_sentiment calculated\n")

    return sentiment_dict


def update_tweet_sentiment(sentiment_dict):
    """
        РѕР±РЅРѕРІР»РµРЅРёРµ Р·РЅР°С‡РµРЅРёР№ tweet_sentiment
    """

    print("updating tweet_sentiment ...")
    cursor = conn.cursor()

    counter_row = 0
    try:
        cursor.execute("begin transaction;")
        for rid, tweet_sentiment in sentiment_dict.items():
            if tweet_sentiment != 0:
                cursor.execute("""UPDATE Tweet
                                     SET tweet_sentiment = ?
                                   WHERE tweet_id = ?;""", (tweet_sentiment, rid))
                counter_row += 1
        conn.commit()
        print("tweet_sentiment updated on {} rows\n".format(counter_row))
    except Exception as msg:
        conn.rollback()
        print("Command skipped: ", msg)


def test_updated_tweet_sentiment():
    """
        Р·Р°РїСЂРѕСЃ Р·РЅР°С‡РµРЅРёР№ tweet_sentiment РёР· Р‘Р”
    """
    print("print updated tweet_sentiment")
    select_data(query="""select tweet_sentiment, count(*) as cnt
                            from tweet
                            where tweet_sentiment != 0
                                    or tweet_sentiment is not null
                            group by tweet_sentiment""")


def main():

    # СѓРґР°Р»РµРЅРёРµ С‚Р°Р±Р»РёС†С‹, РґР»СЏ РїСЂРѕРІРµСЂРєРё
    drop_table()

    # СѓРґР°Р»РµРЅРёРµ РґР°РЅРЅС‹С…, РґР»СЏ РїСЂРѕРІРµСЂРєРё
    # delete_all_data()

    # СЃРѕР·РґР°РЅРёРµ С‚Р°Р±Р»РёС†С‹ tweet
    create_table()

    # Р·Р°РіСЂСѓР·РєР° С‚РІРёС‚РѕРІ РІ Р‘Р”
    load_tweet(file_name=TWEETS_FILE)

    # РґРѕР±Р°РІР»РµРЅРёРµ РєРѕР»РѕРЅРєРё tweet_sentiment РІ Р‘Р”
    add_column_sentiment()

    # select_data(query = """select * from tweet limit 5""")
    # select_data(query = """select * from user limit 5""")

    # СЂР°СЃС‡РµС‚ Р·РЅР°С‡РµРЅРёР№ tweet_sentiment
    # variant=1 С‚РІРёС‚ Р±СЉРµС‚СЃСЏ РЅР° СЃР»РѕРІР° Рё СЃРјРѕС‚СЂСЏС‚СЃСЏ РІ afinn dict (Р±РѕР»РµРµ Р±С‹СЃС‚СЂС‹Р№)
    # variant=2 РїСЂРѕС…РѕРґ РїРѕ afinn dict Рё СЃРјРѕС‚СЂСЏС‚СЃСЏ РЅР° РІС…РѕР¶РґРµРЅРёРµ РІ С‚РІРёС‚Рµ (РјРµРЅРµРµ Р±С‹СЃС‚СЂС‹Р№)
    sentiment_dict_for_update = calculate_tweet_sentiment(variant=2)

    # РѕР±РЅРѕРІР»РµРЅРёРµ Р·РЅР°С‡РµРЅРёР№ tweet_sentiment РІ Р‘Р”
    update_tweet_sentiment(sentiment_dict_for_update)

    # РїСЂРѕРІРµСЂРєР° tweet_sentiment
    test_updated_tweet_sentiment()

    # Р·Р°РєСЂС‹С‚РёРµ СЃРѕРµРґРёРЅРµРЅРёСЏ
    conn.close()

    # ---> Р·Р°РїСѓСЃС‚РёС‚СЊ С„Р°Р№Р» РЅРѕСЂРјР°Р»РёР·Р°С†РёРё normalize.sql

    # ---> Р·Р°РїСѓСЃС‚РёС‚СЊ С„Р°Р№Р» fortunate_country.sql


if __name__ == "__main__":
    main()
