import sqlite3
import asyncio
import math
import random

import pytz as pytz
from datetime import datetime, timedelta

import os
from loguru import logger
from vkbottle import API
from vkbottle import LoopWrapper

from webservice import keep_alive

logger.add("debug.log", format="{time} {level} {message}", level="DEBUG", rotation="100 MB")

LoopW = LoopWrapper()
logger.info(f"Объект LoopWrapper создан: {LoopW}")

token = os.environ['TOKEN_VK']

user = API(token=token)
logger.info(f"Юзер авторизован: {user}")


@logger.catch()
@LoopW.interval(minutes=44)
async def update_bd():
    time_last = None
    result = None
    time_now = datetime.now().date().weekday()

    with open('Conducting a database integrity check.txt', 'r', encoding='UTF-8') as file_chek:

        info = file_chek.readline()
        time_last = info.split(';')[0]
        result = info.split(';')[1]

        logger.info(f"Время последней проверки: {time_last}")
        logger.info(f"Статус последней проверки: {result}")
        logger.info(f"День недели сейчас: {time_now}\n")

        try:
            await user.messages.send(
                user_ids=[658308415],
                random_id=random.randint(-2147483648, 2147483647),
                message=f"Время последней проверки: {time_last}, Статус последней проверки: {result},"
                        f" День недели сейчас: {time_now}\n")

        except Exception as error:
            logger.info(f"Ошибка при отправке сообщения: {error}")

    if time_last != str(time_now) and result == '1':
        logger.info(f"Время проверки прошло, изменяю результат для последующей проверки.")

        try:
            await user.messages.send(
                user_ids=[658308415],
                random_id=random.randint(-2147483648, 2147483647),
                message=f"Время проверки прошло, изменяю результат для последующей проверки.")

        except Exception as error:
            logger.info(f"Ошибка при отправке сообщения: {error}")

        with open('Conducting a database integrity check.txt', 'w', encoding='UTF-8') as file_chek:
            file_chek.write(f"5;0")

    if time_last == str(time_now) and result != '1':

        try:
            await user.messages.send(
                user_ids=[658308415],
                random_id=random.randint(-2147483648, 2147483647),
                message=f"Начал проверку базы данных - {datetime.now(pytz.timezone('Europe/Moscow')).time()} - "
                        f"{datetime.now().date()}")
        except Exception as error:
            logger.info(f"Ошибка при отправке сообщения: {error}")

        _users_vk = await user.friends.get()
        _users_vk_count = _users_vk.count
        logger.info(f"Количество друзей пользователя: {_users_vk_count}")

        _clear_users_vk = []

        for _ in range(math.ceil(int(_users_vk_count) / 100)):

            _item = await user.friends.get(offset=_ * 100, count=100, fields=['bdate'])
            logger.info(f'Получил 100 пользователей из друзей: \n{_item.items}\n')

            await asyncio.sleep(0.1)

            for _1 in _item.items:

                _user_vk_selected_info = {
                    'id': _1.id,
                    'name': _1.first_name,
                    'deactivated': _1.deactivated,
                    'bdate': _1.bdate
                }

                logger.info(f"Анализирую пользователя: {_user_vk_selected_info}")

                if not _1.deactivated:
                    logger.info(
                        f"Со страницей пользователя {_user_vk_selected_info} все прекрасно! Продолжаю анализ дальше.")
                    if _1.bdate:
                        logger.info(
                            f"Дата рождения пользователя {_user_vk_selected_info} известна.")

                        _clear_users_vk.append({
                            'id': _1.id,
                            'name': _1.first_name,
                            'bdate': f"{str(_1.bdate).split('.')[0]}.{str(_1.bdate).split('.')[1]}",
                            'cong_not_cong': 0,
                        })

                        logger.debug(f"Пользователь {_user_vk_selected_info} добавлен в массив!\n")
                    else:
                        logger.critical(f"Дата рождения пользователя {_user_vk_selected_info} неизвестна!\n")
                else:
                    logger.critical(f"Страница пользователя {_user_vk_selected_info} деактивирована!\n")

                await asyncio.sleep(0.1)

        logger.info(f"Сформирован массив активных пользователей с известными датами рождения: \n {_clear_users_vk}")

        with sqlite3.connect('users.db') as conn:
            logger.info(f"База данных открыта: {conn}")
            cur = conn.cursor()
            logger.info(f"Курсор получен: {cur}")
            logger.info(f"______________________________")

            cur.execute('''
            CREATE TABLE IF NOT EXISTS users_day (
            user_id TEXT,
            name TEXT,
            bdate TEXT,
            cong_not_cong TEXT
            )
            ''')

            _table_data = cur.execute(""" SELECT rowid, * FROM users_day""").fetchall()

            if _table_data:
                _count_users_bd = cur.execute('SELECT Count(rowid) FROM users_day').fetchone()[0]

                logger.info(f"\nЧисло полученных пользователей вк: {len(_clear_users_vk)}\n"
                            f"Число пользователей в таблице: {_count_users_bd}")

                if _count_users_bd != len(_clear_users_vk):
                    logger.critical(
                        f"Количество полученных пользователей не совпадает с количеством пользователей в таблице!")

                    try:
                        await user.messages.send(
                            user_ids=[658308415],
                            random_id=random.randint(-2147483648, 2147483647),
                            message=f"Количество полученных пользователей не совпадает с количеством пользователей "
                                    f"в таблице!")
                    except Exception as error:
                        logger.info(f"Ошибка при отправке сообщения: {error}")

                    logger.debug("Начинаю проверку пользователей")
                    for vk_user in _clear_users_vk:

                        _user_presence_table = cur.execute(f"""
                            SELECT EXISTS(SELECT * FROM users_day where user_id = {vk_user['id']})
                        """).fetchone()

                        logger.info(f"Нахождение пользователя ({vk_user['id']}) в таблице: {_user_presence_table}")

                        if not _user_presence_table[0]:
                            cur.execute(f"""
                                INSERT INTO users_day (user_id, name, bdate, cong_not_cong) VALUES (?, ?, ?, ?)
                            """, (f"{vk_user['id']}", f"{vk_user['name']}", f"{vk_user['bdate']}",
                                  f"{vk_user['cong_not_cong']}"))

                            logger.debug(f"Пользователь {vk_user} добавлен в базу данных!")

                            try:
                                await user.messages.send(
                                    user_ids=[658308415],
                                    random_id=random.randint(-2147483648, 2147483647),
                                    message=f"Пользователь {vk_user} добавлен в базу данных!")

                            except Exception as error:
                                logger.info(f"Ошибка при отправке сообщения: {error}")

                            _count_users_bd = cur.execute('SELECT Count(rowid) FROM users_day').fetchone()[0]

                            logger.info(f"\nЧисло полученных пользователей вк: {len(_clear_users_vk)}\n"
                                        f"Число пользователей в таблице: {_count_users_bd}")
            else:

                logger.critical(f"Таблица пустая!")

                try:
                    await user.messages.send(
                        user_ids=[658308415],
                        random_id=random.randint(-2147483648, 2147483647),
                        message=f"Таблица пустая!")

                except Exception as error:
                    logger.info(f"Ошибка при отправке сообщения: {error}")

                logger.debug("Начинаю заполнение!")

                for vk_user in _clear_users_vk:
                    logger.info(f"Пользователь {vk_user} добавляется в базу данных")

                    cur.execute(f"""
                        INSERT INTO users_day (user_id, name, bdate, cong_not_cong) VALUES (?, ?, ?, ?)
                    """, (f"{vk_user['id']}", f"{vk_user['name']}", f"{vk_user['bdate']}",
                          f"{vk_user['cong_not_cong']}"))

                    logger.debug(f"Пользователь {vk_user} добавлен в базу данных!\n")
                    await asyncio.sleep(0.1)

            conn.commit()

            with open('Conducting a database integrity check.txt', 'w', encoding='UTF-8') as file_proverki:

                file_proverki.write(f"5;1")

                logger.info(
                    f"Проверка проведена, заношу день недели проверки - 5"
                    f"и показатель проведения проверки - 1 в файл: {file_proverki.name}")

                try:
                    await user.messages.send(
                        user_ids=[658308415],
                        random_id=random.randint(-2147483648, 2147483647),
                        message=f"Проверка проведена, заношу день недели проверки - 5")

                except Exception as error:
                    logger.info(f"Ошибка при отправке сообщения: {error}")

            logger.info(f"База данных закрыта")


@logger.catch()
@LoopW.interval(minutes=14)
async def happy_birthday():
    _month_now = datetime.now().date().month
    _day_now = datetime.now().date().day
    _day_last = (datetime.now() - timedelta(days=1)).day

    logger.info(f"Месяц сейчас: {_month_now}")
    logger.info(f"День сейчас: {_day_now}")
    logger.info(f"Предыдущий день: {_day_last}\n")

    try:
        await user.messages.send(
            user_ids=[658308415],
            random_id=random.randint(-2147483648, 2147483647),
            message=f"Месяц сейчас: {_month_now}\nДень сейчас: {_day_now}\nПредыдущий день: {_day_last}")

    except Exception as error:
        logger.info(f"Ошибка при отправке сообщения: {error}")

    with sqlite3.connect('users.db') as conn:
        logger.info(f"База данных открыта: {conn}")
        cur = conn.cursor()
        logger.info(f"Курсор создан: {cur}\n")

        _user_info_day = cur.execute(f"""
                SELECT rowid, * FROM users_day WHERE bdate={_day_now}.{_month_now}
        """).fetchall()

        _user_info_day_last = cur.execute(f"""
                SELECT rowid, * FROM users_day WHERE bdate={_day_last}.{_month_now}
        """).fetchall()

        logger.info(f"Сегодня ({_day_now}.{_month_now}) день рождения празднуют следующие люди: {_user_info_day}")
        logger.info(
            f"Вчера ({_day_last}.{_month_now}) день рождения праздновали следующие люди: {_user_info_day_last}\n")

        try:
            await user.messages.send(
                user_ids=[658308415],
                random_id=random.randint(-2147483648, 2147483647),
                message=f"Сегодня ({_day_now}.{_month_now}) день рождения празднуют следующие люди: {_user_info_day}\n"
                        f"Вчера ({_day_last}.{_month_now}) день рождения праздновали следующие люди:"
                        f" {_user_info_day_last}")

        except Exception as error:
            logger.info(f"Ошибка при отправке сообщения: {error}")

        for user_info_last in _user_info_day_last:
            cur.execute("""
                UPDATE users_day SET cong_not_cong = 0 WHERE rowid = ?
            """, (str(user_info_last[0]),))

            logger.info(f"Установил пользователю {user_info_last} cong_not_cong в позицию 0\n")

        for user_info in _user_info_day:
            if user_info[4] != '1':
                try:
                    await user.messages.send(
                        user_ids=[int(user_info[1])],
                        random_id=random.randint(-2147483648, 2147483647),
                        message=f"Добрейшего времени суток, {user_info[2]}! Поздравляю с Днем Рождения и желаю всего "
                                f"самого наилучшего! 👍🏻")

                    await user.messages.send(
                        user_ids=[int(user_info[1])],
                        random_id=random.randint(-2147483648, 2147483647),
                        sticker_id=88078
                    )

                    await user.messages.send(
                        user_ids=[658308415],
                        random_id=random.randint(-2147483648, 2147483647),
                        message=f"Добрейшего времени суток, {user_info[2]}! Поздравляю с Днем Рождения и желаю всего "
                                f"самого наилучшего! 👍🏻")

                    await user.messages.send(
                        user_ids=[658308415],
                        random_id=random.randint(-2147483648, 2147483647),
                        sticker_id=88078
                    )

                    cur.execute("""
                            UPDATE users_day SET cong_not_cong = 1 WHERE rowid = ?
                        """, (str(user_info[0]),))

                    logger.info(f"Установил пользователю {user_info} cong_not_cong в позицию 1\n")

                except Exception as error:
                    logger.info(f"Ошибка при отправке сообщения: {error}")

                await asyncio.sleep(1)

        conn.commit()
        logger.info(f"База данных закрыта")


keep_alive()
if __name__ == '__main__':
    LoopW.run_forever()
