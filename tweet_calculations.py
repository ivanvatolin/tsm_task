# -*- coding: utf-8 -*-

import os
import sqlite3
import json
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
        удаление таблиц
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
        создание таблицы TWEET
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
        удаление всех данных
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
        очмстка текста при анализе на входение слова в словарь AFINN
    """
    return text.encode('utf-8', 'ignore').translate(None, junk_chars).decode().strip().lower()


def clean_column(data):
    """
        очмстка данных при вставке в БД
    """
    return html.escape(data.strip())


def clean_word(word):
    """
        очмстка слова при анализе на входение слова в словарь AFINN
    """
    return word.strip().lower()


def load_tweet(file_name):
    """
        чтение твитов из файла и запись их в БД
    """

    print("loading tweets from {} file ...".format(file_name))
    with open(file_name, 'r') as file:
        row_counter = 0

        # получение данных из json и присвоение переменным
        line = file.readline()
        while line:
            tweet = json.loads(line)

            created_at = tweet.get('created_at')
            # загрузка только созданных твитов
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
                # вставка медиа ресурсов, их может быть много
                if media:
                    for item in range(len(media)):
                        display_url = media[item].get('display_url')
                        row = (tweet_id, user_id, clean_column(name), clean_column(tweet_text), \
                               clean_column(country_code), clean_column(display_url), clean_column(lang), \
                               to_datetime(created_at), clean_column(location))

                        # вставка записи
                        insert_one_row(row)
                        row_counter += 1
                else:
                    row = (tweet_id, user_id, clean_column(name), clean_column(tweet_text), clean_column(country_code),\
                           clean_column(display_url), clean_column(lang), to_datetime(created_at), clean_column(location))

                    # вставка записи
                    insert_one_row(row)
                    row_counter += 1

            line = file.readline()
    conn.commit()
    print("file with tweets was read with {} rows\n".format(row_counter))


def add_column_sentiment():
    """
        добавление колонки 'tweet_sentiment' в таблицу 'Tweet'
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
        запрос всех записей таблицы с твитами либо пишем свой запрос в query
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
        загрузка AFINN словаря
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
        расчет значений tweet_sentiment
        variant=1 твит бъется на слова и смотрятся в afinn dict (более быстрый)
        variant=2 проход по afinn dict и смотрятся на вхождение в твите (менее быстрый)
    """

    print("calculating tweet_sentiment ...")

    # загружаем afinn dict
    afinn_data = get_afinn_dict(file_name=AFINN_DICT_FILE)

    cursor = conn.cursor()
    sentiment_dict = {}

    # проходим по словарю и смотрим вхождение слов в твите
    # сделано 2 варианта
    # try:
    for rid, tweet_text in cursor.execute("select tweet_id, tweet_text from tweet;"):
        tweet_text = clean_text(tweet_text)
        if variant == 1:
            # твит бъется на слова и смотрятся в afinn dict (более быстрый, менее качественный)
            for word in clean_text(tweet_text).split(' '):
                word = clean_word(word)
                if word in afinn_data:
                    sentiment_dict[rid] = sentiment_dict.get(rid, [])+[afinn_data.get(word)]
        else:

            # проход по afinn dict и смотрятся на вхождение в твите (менее быстрый, более качественный)
            for word, val in afinn_data.items():
                if word in tweet_text:
                    sentiment_dict[rid] = sentiment_dict.get(rid, [])+[afinn_data.get(word)]
    # except Exception as msg:
    #     print("Command skipped: ", msg)

    # расчитываем средний sentiment
    for rid, values in sentiment_dict.items():
        sentiment_dict[rid] = round(sum(values)/len(values))

    print("tweet_sentiment calculated\n")

    return sentiment_dict


def update_tweet_sentiment(sentiment_dict):
    """
        обновление значений tweet_sentiment
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


def select_updated_tweet_sentiment():
    """
        запрос значений tweet_sentiment из БД
    """
    print("print updated tweet_sentiment")
    select_data(query="""select tweet_sentiment, count(*) as cnt
                            from tweet
                            where tweet_sentiment != 0
                                    or tweet_sentiment is not null
                            group by tweet_sentiment""")

def fortunate_country():
    print("Fortunate_country: ");
    select_data(query = """select t1.name, t1.lang, t1.location, t1.tweet_sentiment as sentiment,
                            coalesce(t2.sentiment_desc,t3.sentiment_desc) as sentiment_desc
                            from tweet t1
                            left join (select max([tweet_sentiment]) as tweet_sentiment, 'best' as sentiment_desc
                                         from tweet) t2 on t1.tweet_sentiment=t2.tweet_sentiment
                            left join (select min([tweet_sentiment]) as tweet_sentiment, 'worst' as sentiment_desc
                                         from tweet) t3 on t1.tweet_sentiment=t3.tweet_sentiment
                            where location is not null
                            and (t2.tweet_sentiment or t3.tweet_sentiment)
                            order by t1.tweet_sentiment""")


def main():

    # удаление таблицы, для проверки
    drop_table()

    # удаление данных, для проверки
    # delete_all_data()

    # создание таблицы tweet
    create_table()

    # загрузка твитов в БД
    load_tweet(file_name=TWEETS_FILE)

    # добавление колонки tweet_sentiment в БД
    add_column_sentiment()

    # select_data(query = """select * from tweet limit 5""")
    # select_data(query = """select * from user limit 5""")

    # расчет значений tweet_sentiment
    # variant=1 твит бъется на слова и смотрятся в afinn dict (более быстрый)
    # variant=2 проход по afinn dict и смотрятся на вхождение в твите (менее быстрый)
    sentiment_dict_for_update = calculate_tweet_sentiment(variant=2)

    # обновление значений tweet_sentiment в БД
    update_tweet_sentiment(sentiment_dict_for_update)

    # проверка tweet_sentiment
    select_updated_tweet_sentiment()

    # запрос лучшей и худшей страны и пользователя
    fortunate_country()

    # закрытие соединения
    conn.close()


    # ---> запустить файл нормализации normalize.sql



if __name__ == "__main__":
    main()
