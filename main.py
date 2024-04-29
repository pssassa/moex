import sqlite3
import pandas as pd
from datetime import date, datetime, time, timedelta
import os.path


import moexalgo as moex

from moexalgo import Market, Ticker


def console_title(title):
    """Функция отображения названия окна консоли"""
    if os.name == "nt":
        # Для Windows используем команду 'title'
        os.system("title " + title)
    else:
        # Для других операционных систем используем команду 'echo'
        os.system("echo -n -e '\033]0;" + title + "\a' > /dev/tty")



def log_plus(text):
    """Функция для записи в логфайл и вывода в консоль"""
    with open(filelog, "a", encoding="utf-8") as logfile:
        # Получаем текущее время
        current_time = datetime.now().replace(microsecond=0)
        # Форматируем строку для записи в лог
        log_entry = f"{current_time}: {text}\n"
        # # Записываем строку в файл
        logfile.write(log_entry)
        print(log_entry, end="")

def stocks_in_db():

    stock = Market('stocks').tickers() #получаем данные по ВСЕМ акциям
    stock_df = pd.DataFrame(stock) #помещаем в датафрейм

    with sqlite3.connect("db.db") as conn:
        cur = conn.cursor()
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS _ALL_stocks (
            SECID TEXT PRIMARY KEY,
            BOARDID TEXT,
            SHORTNAME TEXT,
            PREVPRICE REAL,
            LOTSIZE REAL,
            FACEVALUE REAL,
            STATUS TEXT,
            BOARDNAME TEXT,
            DECIMALS INTEGER,
            SECNAME TEXT,
            REMARKS TEXT,
            MARKETCODE TEXT,
            INSTRID TEXT,
            SECTORID TEXT,
            MINSTEP REAL,
            PREVWAPRICE REAL,
            FACEUNIT TEXT,
            PREVDATE TEXT,
            ISSUESIZE REAL,
            ISIN TEXT,
            LATNAME TEXT,
            REGNUMBER TEXT,
            PREVLEGALCLOSEPRICE REAL,
            CURRENCYID TEXT,
            SECTYPE TEXT,
            LISTLEVEL INTEGER,
            SETTLEDATE TEXT)
            """
        )
        stock_df.to_sql("_ALL_stocks", conn, if_exists="replace", index=False)

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS _ALL_stocks_with_first_candles (
            SECID TEXT PRIMARY KEY,
            FIRST_DATE DATE,
            LISTLEVEL INTEGER,
            SECNAME TEXT)
            """
        )
        first_candles_df = stock_df[["SECID", "LISTLEVEL", "SECNAME"]]

        for secid in stock_df["SECID"]:
            try:
                log_plus(f"Ищу первую свечу по акции {secid}")
                first_candle = pd.DataFrame(Ticker(secid).candles(date=Data_start, till_date="today", period=1, limit=10)).loc[0, "begin"]
                first_candles_df.loc[first_candles_df["SECID"] == secid, "FIRST_DATE"] = first_candle
            except Exception as e:
                log_plus(f"Ошибка получения даты первой свечи по акции {secid}")
                first_candles_df.loc[first_candles_df["SECID"] == secid, "FIRST_DATE"] = pd.NaT

        first_candles_df.to_sql("_ALL_stocks_with_first_candles", conn, if_exists="replace", index=False)
        conn.commit()

def candles_with_last_date(limit, secid, last_date, period, conn, timeframe):
    till_date = datetime.now().replace(microsecond=0)
    count_candle = 0
    real_limit = limit
    while limit == real_limit:
        try:
            candle_df = Ticker(secid).candles(date=last_date, till_date=till_date, period=period, limit=limit)
        except Exception as e:
            log_plus(
                f"Ошибка от Мосбиржи при получении данных для акции {secid} с таймфреймом {period}. "
                f"\nПродолжаем сбор данных.")
            real_limit = 0
            continue

        candle_df = pd.DataFrame(candle_df, columns=[
            "begin",
            "end",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
        ])
        real_limit = candle_df.shape[0]
        if not candle_df.empty:
            count_candle = (candle_df.shape[0] + count_candle)
            candle_df["begin"] = candle_df["begin"] + pd.Timedelta(hours=0, minutes=0, seconds=0)
            candle_df["begin"] = candle_df["begin"].dt.strftime("%Y-%m-%d %H:%M:%S")
            last_date = pd.to_datetime(candle_df.iloc[-1]["begin"])
            last_date = last_date + timeframe
        if count_candle > 1:
            candle_df.to_sql(f"{secid}_{period}", conn, if_exists="append", index=False)
        log_plus(f"Получили данные до {last_date}. Продолжаем скачивать")
        if count_candle > 1:
            log_plus(f"Скачано {count_candle} свечей с таймфреймом {period} для акции '{secid}'")
    return

def candles_without_last_date(limit, secid, first_date, period, conn, timeframe):
    till_date = datetime.now().replace(microsecond=0)
    count_candle = 0
    real_limit = limit
    while limit == real_limit:
        try:
            candle_df = Ticker(secid).candles(date=first_date, till_date=till_date, period=period, limit=limit)
        except Exception as e:
            log_plus(f"Ошибка от Мосбиржи при получении данных для акции {secid} с таймфреймом {period}. "
                f"\nПродолжаем сбор данных.")
            real_limit = 0
            continue

        candle_df = pd.DataFrame(candle_df, columns=[
            "begin",
            "end",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
        ])
        real_limit = candle_df.shape[0]
        if not candle_df.empty:
            count_candle = (candle_df.shape[0] + count_candle)
            candle_df["begin"] = candle_df["begin"] + pd.Timedelta(hours=0, minutes=0,seconds=0)
            candle_df["begin"] = candle_df["begin"].dt.strftime("%Y-%m-%d %H:%M:%S")
            first_date = pd.to_datetime(candle_df.iloc[-1]["begin"])
            first_date = first_date + timeframe
        if count_candle > 1:
            candle_df.to_sql(f"{secid}_{period}", conn, if_exists="append", index=False)
    if count_candle > 1:
        log_plus(f"Скачано {count_candle} свечей с таймфреймом {period} для акции '{secid}'")


def table_exist(table_name, conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    result = cur.fetchone()
    return result is not None

def download_candels():
    limit = 50000
    periods = ["D", "1h", "10m", "1m"]
    timeframes = [
        timedelta(hours=24),
        timedelta(hours=1),
        timedelta(minutes=10),
        timedelta(minutes=1),
    ]

    with sqlite3.connect("db.db") as conn:
        cur = conn.cursor()

        cur.execute("SELECT SECID, FIRST_DATE FROM _ALL_stocks_with_first_candles")
        tuple_list = cur.fetchall()

        for period, timeframe in zip(periods, timeframes):

            for secid, first_date in tuple_list:

                log_plus(f"Занимаемся акцией '{secid}'")

                if first_date is not None:
                    first_date = datetime.strptime(first_date, "%Y-%m-%d %H:%M:%S")
                    table_name = f"{secid}_{period}"
                    if table_exist(table_name, conn):
                        cur.execute(f"SELECT MAX(begin) FROM {secid}_{period}")
                        last_date = cur.fetchone()[0]
                        last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")

                        if last_date is not None and last_date >= datetime.now().replace(hour=0, minute=0, second=0):
                            log_plus(f"Данные для акции '{secid}' с таймфреймом {period} уже актуальны")
                            continue  # Пропустить скачивание данных, если они уже актуальны

                        elif last_date is not None and last_date <= datetime.now().replace(hour=0, minute=0, second=0):
                            log_plus(f"Качаем свечи для акции '{secid}' с таймфреймом {period} начиная с {last_date}")
                            candles_with_last_date(limit, secid, last_date, period, conn, timeframe)
                            conn.commit()

                    else:
                        log_plus(f"Качаем свечи для акции '{secid}' с таймфреймом {period} начиная с {first_date}")
                        candles_without_last_date(limit, secid, first_date, period, conn, timeframe)
                        conn.commit()


if __name__ == "__main__":

    # запоминаем время для записи продолжительности работы программы
    start = datetime.now()
    folder_path = "folder_log"
    filelog = os.path.join(folder_path, "logfile.log") #log file
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        with open(filelog, "a", encoding="utf-8") as logfile:
            log_plus(f"Каталог '{folder_path}' для хранения log-файлов и базы данных успешно создан")
            log_plus("logfile.log создан")
            log_plus("Сканируем доступные акции Мосбиржи.")
            # берем заведомо старую дату, чтобы найти самые ранние свечи (у нас с 22 года, но можно раньше)
            Data_start = datetime.strptime("2022-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            #stocks_in_db()
    else:
        with open(filelog, "a", encoding="utf-8") as logfile:
            log_plus(f"Каталог '{folder_path}' для хранения log-файлов и базы данных уже создан")
            log_plus("Обновляем базу данных.")
            # берем заведомо старую дату, чтобы найти самые ранние свечи (у нас с 22 года, но можно раньше)
            Data_start = datetime.strptime("2022-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            #stocks_in_db()

    download_candels()

    # считаем время работы программы
    time_work = datetime.now() - start
    days = time_work.days
    hours, remainder = divmod(time_work.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    end_time = ""
    if days > 0:
        end_time += f"{days} дней "
    if hours > 0:
        end_time += f"{hours} часов "
    if minutes > 0:
        end_time += f"{minutes} минут "

    log_plus(f"Загрузка заняла {end_time}, спасибо за работу!")
