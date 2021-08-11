import requests
from config import *
import datetime, time
import pymysql

telegram_url = 'https://api.telegram.org/bot' + api_bot
get_updates_url = telegram_url + '/getUpdates?'
db = pymysql.connect(host=host, user=user, password=password, database=name)


def get_updates():
    r = requests.get(get_updates_url)
    print(r.json())
    return r.json()


def get_update_id_from_bd(update_id):
    try:
        print(f"UPDATE_ID_SQL: {update_id}")
        cursor = db.cursor()
        sql_update = update_id
        sql = "SELECT * FROM update_id WHERE update_id = %s"
        cursor.execute(sql, sql_update)
        data_db = cursor.fetchone()
        print(data_db)
        if data_db:
            print("OK")
            return "OK"
    except Exception as e:
        print(e)


def insert_data_base(chat_id, message_from_user, date_message, chat_name, update_id):
    try:
        cursor = db.cursor()
        sql = "INSERT INTO update_id(update_id) VALUES (%s)"
        cursor.execute(sql, update_id)
        db.commit()
        cursor = db.cursor()
        sql = "INSERT INTO data_chats(chat_name, date_message, message_from_user, chat_id) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (chat_name, date_message, message_from_user, chat_id))
        db.commit()

    except Exception as e:
        print(e)


def get_data_edited_message_telegram(json_data_request):
    chat_id = json_data_request['edited_message']['chat']['id']
    message_from_user = json_data_request['edited_message']['from']['first_name']
    date_message = json_data_request['edited_message']['date']
    date_message = datetime.datetime.utcfromtimestamp(date_message)
    chat_name = json_data_request['edited_message']['chat']['title']
    update_id = json_data_request['update_id']
    dict_data = {
        'chat_id': chat_id,
        'message_from_user': message_from_user,
        'date_message': date_message,
        'chat_name': chat_name,
        'update_id': update_id
    }
    return dict_data


def get_data_json_telegram(json_data_request):
    try:
        chat_id = json_data_request['message']['chat']['id']
        message_from_user = json_data_request['message']['from']['first_name']
        date_message = json_data_request['message']['date']
        date_message = datetime.datetime.utcfromtimestamp(date_message)
        chat_name = json_data_request['message']['chat']['title']
        update_id = json_data_request['update_id']
        dict_data = {
            'chat_id': chat_id,
            'message_from_user': message_from_user,
            'date_message': date_message,
            'chat_name': chat_name,
            'update_id': update_id
        }
        return dict_data

    except KeyError as e:
        print(f"ERROR MESSAGE: {e}")
        if str(e) == "'message'":
            return get_data_edited_message_telegram(json_data_request)
        else:
            return "Error"


if __name__ == '__main__':
    json_result = get_updates()
    if json_result['ok']:
        print(json_result['result'])
        for i in json_result['result']:
            data_request = get_data_json_telegram(i)
            if data_request != "Error":
                up_id = data_request['update_id']
                data_sql = (get_update_id_from_bd(up_id))
                if not data_sql:
                    insert_data_base(data_request['chat_id'], data_request['message_from_user'],
                                     data_request['date_message'], data_request['chat_name'],
                                     data_request['update_id'])
