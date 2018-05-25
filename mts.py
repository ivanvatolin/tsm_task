import sqlite3
import json
import html
from datetime import datetime

DB_NAME = 'db.sqlite3'
TWEETS_FILE = 'three_minutes_tweets.json.txt'
AFINN_DICT_FILE = 'AFINN-111.txt'

conn = sqlite3.connect(DB_NAME)


def drop_table():
    """
        drop table
    """

    print("dropping table ...")
    cursor = conn.cursor()
    tables = ['tweet', 'user']

    try:
        for table in tables:
            cursor.execute("DROP TABLE IF EXISTS {};".format(table))
        conn.commit()
        print("table dropped\n")
    except sqlite3.Error as msg:
        print("Command skipped: ", msg)


def create_table():
    """
        creating table
    """

    cursor = conn.cursor()
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS tweet(
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            tweet_id INTEGER,
                            user_id INTEGER,
                            name TEXT,
                            tweet_text TEXT,
                            country_code TEXT,
                            display_url TEXT,
                            lang TEXT,
                            created_at TIMESTAMP,
                            location TEXT);""")
        conn.commit()
    except sqlite3.Error as msg:
        print("Command skipped: ", msg)
    print("table Tweet created\n")


def to_datetime(dt):
    return datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')


def insert_one_row(row):
    """
        insert one row
    """

    tweet_id, user_id, name, tweet_text, country_code, display_url, lang, created_at, location = row

    try:
        cursor = conn.cursor()
        query = """INSERT INTO Tweet(
                tweet_id, user_id, name, tweet_text, country_code, display_url, lang, created_at, location)
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');"""\
                .format(tweet_id, user_id, name, tweet_text, country_code, display_url, lang, to_datetime(created_at), location)
        cursor.execute(query)
    except sqlite3.Error as msg:
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


def clean_data(data):
    return html.escape(data.strip())


def clean_word(word):
    return word.strip().lower()


def load_tweet(file_name):
    """
        read tweets from file and insert those into DB
    """

    print("loading tweets ...")
    with open(file_name, 'r') as file:
        row_counter = 0

        # получение данных из json
        line = file.readline()
        while line:
            tweet = json.loads(line)

            created_at = tweet.get('created_at')

            if created_at:
                tweet_id = tweet.get("id")
                user = tweet.get("user")
                user_id = user.get("id")
                name = user.get("name")
                tweet_text = tweet.get('text')
                place = tweet.get("place")
                country_code = place.get('country_code') if tweet.get("place") else ''
                media = tweet.get('extended_entities').get('media') if tweet.get('extended_entities') else ''
                display_url = media[0].get('display_url','') if media else ''
                lang = user.get('lang')
                location = user.get('location')
                row = (tweet_id, user_id, clean_data(name), clean_data(tweet_text), clean_data(country_code), clean_data(display_url), clean_data(lang), created_at, clean_data(location))

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
    except sqlite3.Error as msg:
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

    print("AFINN file was read with {} rows\n".format(len(dic)))
    return dic


def calculate_tweet_sentiment(variant=1):
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
    try:
        for rid, tweet_sentiment in cursor.execute("select id, tweet_text from tweet;"):
            if variant == 1:

                # твит бъется на слова и смотрятся в afinn dict (более быстрый)
                for word in tweet_sentiment.split(' '):
                    word = clean_word(word)
                    if word in afinn_data:
                        sentiment_dict[rid] = sentiment_dict.get(rid, [])+[afinn_data.get(word)]
            else:

                # проход по afinn dict и смотрятся на вхождение в твите (менее быстрый)
                for word, val in afinn_data.items():
                    if word in tweet_sentiment:
                        sentiment_dict[rid] = sentiment_dict.get(rid, [])+[afinn_data.get(word)]
    except sqlite3.Error as msg:
        print("Command skipped: ", msg)

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
                                      SET tweet_sentiment='{}'
                                    WHERE id = '{}';""".format(tweet_sentiment, rid))
                counter_row += 1
        conn.commit()
        print("tweet_sentiment updated on {} rows\n".format(counter_row))
    except sqlite3.Error as msg:
        conn.rollback()
        print("Command skipped: ", msg)




def test_updated_tweet_sentiment():
    """
        запрос значений tweet_sentiment из БД
    """
    print("print updated tweet_sentiment");
    select_data(query = """select tweet_sentiment, count(*) as cnt
                            from tweet
                            where tweet_sentiment != 0
                                    or tweet_sentiment is not null
                            group by tweet_sentiment""")


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

    # ---> запустить файл нормализации normalize.sql
    # select_data(query = """select * from tweet limit 5""")
    # select_data(query = """select * from user limit 5""")

    # расчет значений tweet_sentiment
    # variant=1 твит бъется на слова и смотрятся в afinn dict (более быстрый)
    # variant=2 проход по afinn dict и смотрятся на вхождение в твите (менее быстрый)
    sentiment_dict_for_update = calculate_tweet_sentiment(variant=2)

    # обновление значений tweet_sentiment в БД
    update_tweet_sentiment(sentiment_dict_for_update)

    # проверка tweet_sentiment
    test_updated_tweet_sentiment()

    # закрытие соединения
    conn.close()


if __name__ == "__main__":
    main()
