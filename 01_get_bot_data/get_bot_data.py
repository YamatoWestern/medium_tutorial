import datetime, json, os, logging, requests
from ratelimit import limits, sleep_and_retry

logging.basicConfig(level=logging.INFO)

# กำหนดว่าเริ่มดึงตั้งแต่วันที่ 2002-01-01
start_date_str = '2002-01-01'
date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")

# ให้เก็บข้อมูลไว้ที่ folder ชื่อว่า data
dir_path = 'data'
if not os.path.exists(dir_path):
    os.mkdir(dir_path)

# เขียน fn ที่ใช้ดึงข้อมูล โดยการใส่วันที่
@sleep_and_retry
@limits(calls=1, period=datetime.timedelta(seconds=1).total_seconds()) 
def get_bot_data(input_date):
    try:
        # ทำ datetime ให้กลายเป็น data time string format 2002-01-01
        date_str = input_date.strftime('%Y-%m-%d')
        # ตรงนี้ดักเพื่อว่า จะได้ไม่โหลดข้อมูลที่เคยโหลดมาแล้วซ้ำ
        path = os.path.join(dir_path, f'{date_str}.json')
        if not os.path.exists(path):
            url = f'https://www.bot.or.th/content/bot/th/statistics/exchange-rate/jcr:content/root/container/statisticstable2.results.level3cache.{date_str}.json'
            r = requests.get(url, timeout=5) # ให้ timeout 5 วินาที
            resp = r.json()
            with open(path, 'w') as f:
                json.dump(resp, f, ensure_ascii=False, indent=4)
            logging.info(f'Load data -> {date_str} : SUCCESS')
        else:
            logging.info(f'Skip {date_str}')
    except requests.exceptions.Timeout:
        logging.error(f'Load data -> {date_str} : The request timed out')
    except Exception as e:
        logging.exception(f'Load data -> {date_str} : ERROR')

# โดยให้เริ่มดึงจนถึึงวันที่ ปัจจุบัน
while date <= datetime.datetime.now():
    date_str = date.strftime('%Y-%m-%d')
    logging.info(f'Load data -> {date_str}')
    get_bot_data(date)
    # ทำไปเลยทีละวัน
    date += datetime.timedelta(days=1)