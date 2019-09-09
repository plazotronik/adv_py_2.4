#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pymongo import MongoClient, ASCENDING
import csv
from pprint import pprint
from datetime import datetime
from urllib.parse import quote_plus


def mongo_connect(login='', password='', server='localhost', port=27017, db=''):
    if login:
        url = "mongodb://%s:%s@%s:%s/%s" % (quote_plus(login), quote_plus(password),
                                            quote_plus(server), port, quote_plus(db))
    else:
        url = "mongodb://%s:%s/%s" % (quote_plus(server), port, quote_plus(db))
    try:
        client = MongoClient(url)
        client.admin.command('ismaster')
    except Exception as err:
        print("Could not connect to MongoDB. Detailed: ", err)
    else:
        print("Connected successfully!!!")
        return client


def read_data(path_to_file, db_object):
    with open(path_to_file, encoding='utf8') as file:
        row = csv.DictReader(file)
        if not list(db_object.find()):
            db_object.insert_many(row)


def format_data(db_object, column_price='Цена', column_date='Дата'):
    for doc in db_object.find():
        db_object.find_one_and_update({'_id': doc['_id'], column_price: {'$type': 'string'}},
                                      {'$set': {column_price: int(doc[column_price])}})
        if isinstance(doc[column_date], str):
            raw_data = doc[column_date]
            list_data = [i if (len(i) == 2) else f'0{i}' for i in raw_data.split('.')]
            str_data = raw_data if len(raw_data) == 5 else list_data
            try:
                data_ = datetime.fromisoformat('2019-{0[1]}-{0[0]}'.format(str_data))
                db_object.find_one_and_update({'_id': doc['_id'], column_date: {'$type': 'string'}},
                                              {'$set': {column_date: data_}})
            except ValueError as err:
                print('Error in value date. Detailed: ', err)
            except Exception as err:
                print(err)


def find_cheapest(db_object, column_find='Цена'):
    sort = db_object.find().sort(column_find, ASCENDING)
    pprint(list(sort))


def find_by_name(db_object, name_find, column_find, column_sort='Цена'):
    artist = db_object.find({column_find: {'$regex': '.*%s.*' % name_find,
                                           '$options': 'i'}}).sort(column_sort, ASCENDING)
    pprint(list(artist))


def find_by_date(db_object, start_date, end_date, column_find='Дата'):
    try:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        data_ = db_object.find({column_find: {'$gte': start, '$lte': end}}
                               ).sort(column_find, ASCENDING)
        pprint(list(data_))
    except ValueError as err:
        print('Error in value date. Detailed: ', err)
    except Exception as err:
        print(err)


if __name__ == '__main__':
    # input your credentials
    conn = mongo_connect(server='it-vi.ru', port=33827)
    concerts = conn['netology_db']['concerts']
    read_data('artists.csv', concerts)
    format_data(concerts)

    # sort by price
    find_cheapest(concerts)
    print(f'{"=" * 90}')

    # search by artist
    find_by_name(concerts,'on', 'Исполнитель')
    print(f'{"=" * 90}')

    # search by location
    find_by_name(concerts, 'st', 'Место')
    print(f'{"=" * 90}')

    # sort by date for a specific period
    find_by_date(concerts, '2019-07-01', '2019-07-30')
    conn.close()
