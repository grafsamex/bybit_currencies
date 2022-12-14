# pip install pybit
'''trading_pair_in_bybit - функционал:
позволяет подключиться в бирже криптовалют Bybit, взять все имеющиеся торговые
пары и сравнить со списком в базе данных. В случае наличия расхождений - позволяет
привется базу данных в соответствии со значениями на бирже'''
from pybit import spot
import datetime
import sqlite3 as sl


def data_verification(a, b):    #Сравнение списков
    return [x for x in a if x not in b]


def insert_bybit_currency(missing, cursor, connect_db): #Добавление данных в таблицу торговых пар
    for i in range(len(missing)):
        quote_pair = f"INSERT INTO bybit_currency (name, base_currency, quote_currency) VALUES ('{missing[i][0]}', '{missing[i][1]}', '{missing[i][2]}')"
        cursor.execute(quote_pair)
        connect_db.commit()



def delete_bybit_currency(missing, cursor, connect_db): #Удаление данных из таблицы торговых пар
    missing = list(set([x[0] for x in missing]))
    for i in range(len(missing)):
        delete_pair = f"DELETE FROM bybit_currency WHERE name = '{missing[i]}'"
        cursor.execute(delete_pair)
        connect_db.commit()


def currency_pair_bybit():    #Подключение к BYBIT с отлавливанием ошибки подключения
    while True:
        try:
            session_unauth = spot.HTTP(endpoint="https://api.bybit.com")
            print('Серверное время BYBIT: ', datetime.datetime.fromtimestamp(float(session_unauth.server_time()['time_now'])))
            print(f"Всего торговых пар на площадке BYBIT: {len(session_unauth.query_symbol()['result'])}")
            bybit_cur = session_unauth.query_symbol()['result']
            break
        except:
            print('Подключение не удалось выполнить. Повторить попытку?')
            connect_bybit = input('Для повторной попытки нажмите любую клавишу. Для прерывания операции введите "NO": ')
            print(connect_bybit)
            if connect_bybit.lower() == 'no' or connect_bybit.lower() == 'тщ':
                return
            else:
                continue
    #Выкачивание всех торговых пар и преобразование их в список
    bybit_cur_list = [[bybit_cur[i]['name'], bybit_cur[i]['baseCurrency'], bybit_cur[i]['quoteCurrency']] for i in range(len(bybit_cur))]
    #print(bybit_cur_list)
    # Подключаемся к базе данных и смотрим все торговые пары, имеющиеся у нас в базе.
    connect_db = sl.connect(r'../criptobase.db')
    cursor = connect_db.cursor()
    #проверка существования таблица и создание, если отсутствует
    cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='bybit_currency'")
    if cursor.fetchone()[0] == 0:
        cursor.execute("CREATE TABLE bybit_currency(bybit_currency_id INT PRIMARY KEY, name TEXT, base_currency TEXT, quote_currency TEXT)")
        connect_db.commit()
    cursor.execute('SELECT * FROM bybit_currency')
    db_currency_tuple = cursor.fetchall()
    db_currency_list = [[db_currency_tuple[i][1], db_currency_tuple[i][2], db_currency_tuple[i][3]] for i in range(len(db_currency_tuple))]
    #print(db_currency_list)
    #Значения, отсутствующие в БД
    missing_db = data_verification(bybit_cur_list, db_currency_list)
    #Значения, отсутствующие на площадке
    missing_bybit = data_verification(db_currency_list, bybit_cur_list)
    if len(missing_bybit)+ len(missing_db) == 0:
        print('Изменений не найдено')
        cursor.close()
        connect_db.close()
        return
    print(f'Отсутствуют значения в базе данных: {len(missing_db)}')
    print(missing_db)
    print(f'Отсутствуют значения на BYBIT: {len(missing_bybit)}')
    print(missing_bybit)
    make_change_bybit = input('Какие изменения внести в базу данных: \n 1 - Внести все изменения \n 2 - Добавить данные в базу данных \n 3 - Удалить данные из базы данных \n 4 - Не вносить изменения \n')
    if make_change_bybit == '1':
        insert_bybit_currency(missing_db, cursor, connect_db)
        delete_bybit_currency(missing_bybit, cursor, connect_db)
        cursor.close()
        connect_db.close()
        print('Изменения успешно внесены')
        return
    elif make_change_bybit == '2':
        insert_bybit_currency(missing_db, cursor, connect_db)
        cursor.close()
        connect_db.close()
        print('Данные внесены в базу данных')
        return
    elif make_change_bybit == '3':
        delete_bybit_currency(missing_bybit, cursor, connect_db)
        cursor.close()
        connect_db.close()
        print('Данные удалены из базы данных')
        return
    elif make_change_bybit == '4':
        cursor.close()
        connect_db.close()
        print('Изменения не внесены')
        return
    else:
        cursor.close()
        connect_db.close()
        print('Введено некорректное значение, изменения не внесены.')
        return



if __name__ == '__main__':
    currency_pair_bybit()
    print('База торговых пар Bybit актуальна>')
