
import os
import logging
from concurrent.futures import ThreadPoolExecutor
import pymssql
import requests
import time
import random
from typing import List, Dict, Optional, Any
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import ddddocr
from dotenv import load_dotenv

load_dotenv() # 載入 .env 檔案中的環境變數

# --- 全域常數設定 (Global Constants) ---
# 檔案相關
COOKIE_FILE = 'tii_elearning_cookies.txt'
LOG_FILE = 'sync.log'

# URL 相關
BASE_URL = 'https://elearning.tii.org.tw'
LOGIN_URL = f'{BASE_URL}/edu/mpage/'
API_URL = f'{BASE_URL}/moodle/company/ajax_list.php?api=complete_status_company_detail'

# Playwright 相關 CSS 選擇器
USERNAME_SELECTOR = '#username'
PASSWORD_SELECTOR = '#password'
CAPTCHA_IMG_SELECTOR = '#captcha_img'
CAPTCHA_CODE_SELECTOR = '#captcha_code'
SIGNIN_BUTTON_SELECTOR = '.btn-signin'
ERROR_ALERT_SELECTOR = '.alert.alert-danger'

# 執行緒與超時設定
MAX_WORKERS = 5
REQUEST_TIMEOUT = 30

# --- 日誌設定 (Logging Configuration) ---
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sync.log')
# 修改日誌設定，使其同時輸出到檔案和控制台
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler() # 新增 StreamHandler 以輸出到控制台
    ]
)



def save_cookie(cookie_str: str):
    """將 Cookie 字串儲存到檔案"""
    try:
        with open(COOKIE_FILE, 'w', encoding='utf-8') as f:
            f.write(cookie_str)
        logging.info("Cookie 已成功儲存。")
    except IOError as e:
        logging.error(f"儲存 Cookie 失敗: {e}")

def get_cookie() -> Optional[str]:
    """從檔案讀取 Cookie"""
    if not os.path.exists(COOKIE_FILE):
        return None
    try:
        with open(COOKIE_FILE, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except IOError as e:
        logging.error(f"讀取 Cookie 失敗: {e}")
        return None

def clear_cookies():
    """清除本地儲存的 Cookie 檔案"""
    if os.path.exists(COOKIE_FILE):
        try:
            os.remove(COOKIE_FILE)
            logging.info("Cookie 檔案已清除。")
        except OSError as e:
            logging.error(f"清除 Cookie 檔案時發生錯誤: {e}")

def _attempt_login(page, ocr, username, password) -> bool:
    """
    執行單次登入嘗試。
    :return: 是否成功
    """
    try:
        page.goto(LOGIN_URL, timeout=60000)

        page.fill(USERNAME_SELECTOR, username)
        page.fill(PASSWORD_SELECTOR, password)

        # 處理驗證碼
        captcha_element = page.query_selector(CAPTCHA_IMG_SELECTOR)
        if not captcha_element:
            logging.error("找不到驗證碼圖片元素。")
            return False
        
        img_bytes = captcha_element.screenshot()
        captcha_text = ocr.classification(img_bytes)
        logging.info(f"OCR 辨識結果: {captcha_text}")

        page.fill(CAPTCHA_CODE_SELECTOR, captcha_text)

        # 點擊登入並等待頁面導航
        page.click(SIGNIN_BUTTON_SELECTOR)

        # 使用更可靠的方式判斷登入結果
        # 1. 等待 URL 變化 (成功)
        # 2. 或者等待錯誤訊息出現 (失敗)
        page.wait_for_load_state('networkidle', timeout=5000)

        if "mpage" not in page.url:
            logging.info("登入成功！URL 已變更。")
            cookies = page.context.cookies()
            cookie_str = '; '.join([f"{c['name']}={c['value']}" for c in cookies])
            save_cookie(cookie_str)
            return True
        
        # 檢查是否有錯誤提示
        error_element = page.query_selector(ERROR_ALERT_SELECTOR)
        if error_element:
            logging.warning(f"登入失敗: {error_element.inner_text()}")
        return False
    except PlaywrightTimeoutError:
        logging.warning("等待頁面載入超時，可能登入失敗或網路延遲。")
        return False

# --- 登入主函式 (Main Login Function) ---
def login_and_save_cookie(max_attempts: int = 10) -> bool:
    """
    使用 Playwright 和 ddddocr 登入 TII eLearning 平台並儲存 Cookie。
    :param max_attempts: 最大嘗試次數
    :return: 是否登入成功
    """
    username = os.environ.get('TII_USERNAME')
    password = os.environ.get('TII_PASSWORD')
    ocr = ddddocr.DdddOcr()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            for attempt in range(max_attempts):
                logging.info(f"正在嘗試登入，第 {attempt + 1}/{max_attempts} 次...")
                page = browser.new_page()
                if _attempt_login(page, ocr, username, password):
                    return True
                page.close() # 關閉當前頁面，下次迴圈開新的
                time.sleep(random.uniform(2, 4)) # 每次失敗後稍作等待
        except Exception as e:
            logging.error(f"登入過程中發生未知錯誤: {e}")
        finally:
            browser.close()

    logging.error("所有登入嘗試均失敗。")
    return False

# --- 核心同步邏輯 (Core Synchronization Logic) ---
def sync_data(item: Dict, cookie_str: str) -> bool:
    """
    同步單條資料到資料庫
    :param item: 包含銷售登記ID和日期範圍的資料字典
    :param cookie_str: 用於驗證的 Cookie
    :return: 是否同步成功
    """
    headers = {
        'cookie': cookie_str,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }

    try:
        response = requests.post(
            API_URL,
            headers=headers,
            data={
                'salesregid': item['salesregid'],
                'finish_start_date': item['finish_start_date'],
                'finish_end_date': item['finish_end_date']
            },
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        api_data = response.json()
        if 'total' not in api_data or 'rows' not in api_data:
            logging.error(f"API 回應格式不正確: {api_data}")
            # Cookie 可能已失效
            clear_cookies()
            return False

        if api_data['total'] == item['nTotalComplete']:
            logging.info(f"資料未變化，跳過: {item['salesregid']} (數量: {api_data['total']})")
            return True

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                delete_details(cursor, item)
                insert_details(cursor, item, api_data['rows'])
                update_summary(cursor, item, api_data['total'])
        return True

    except requests.exceptions.RequestException as e:
        logging.error(f"API請求失敗: {item['salesregid']} - {e}")
        clear_cookies()  # API 失敗時清除 cookie，強制下次重新登入
    except pymssql.Error as e:
        logging.error(f"資料庫操作失敗: {item['salesregid']} - {e}")
    except Exception as e:
        logging.error(f"未知錯誤: {item['salesregid']} - {e}")

    return False

def get_db_connection():
    """获取数据库连接"""
    return pymssql.connect(
        server=os.environ.get('DB_SERVER'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        database=os.environ.get('DB_NAME'),
        autocommit=True,
        timeout=60
    )

def delete_details(cursor, item: Dict):
    """刪除指定條件的舊明細資料"""
    stmt = "DELETE FROM NYDB.AT.InsuExternalTrainingY WHERE cInsuLicense = %s AND dChgDate >= %s AND dChgDate <= %s"
    params = (item['salesregid'], item['dTrainBeginDate'], item['dTrainEndDate'])
    cursor.execute(stmt, params)
    logging.info(f"已刪除舊明細紀錄: {item['salesregid']} 課程年月: {item['cClassYM']}")

def insert_details(cursor, item: Dict, rows: List[Dict]):
    """批量插入明细数据"""
    stmt = """
        INSERT INTO NYDB.AT.InsuExternalTrainingY (
            cClassYM, cInsuLicense, cEmpIdn, cCourse, dChgDate
        ) VALUES (
            %s, %s, NYDB.AT.getEmpIdnByInsuLicence(%s), %s, %s
        )
    """
    params = [
        (item['cClassYM'], item['salesregid'], item['salesregid'], row['fullname'], row['finish_time'])
        for row in rows
    ]
    if not params:
        logging.info(f"無新明細可新增: {item['salesregid']}")
        return
    cursor.executemany(stmt, params)
    logging.info(f"已新增 {len(params)} 條新明細紀錄: {item['salesregid']} 課程年月: {item['cClassYM']}")

def update_summary(cursor, item: Dict, total: int):
    """更新汇总数据"""
    stmt = "UPDATE NYDB.AT.InsuExternalTrainingX SET nTotalComplete = %s, dRefreshDate = GETDATE() WHERE cInsuLicense = %s AND dTrainBeginDate = %s AND dTrainEndDate = %s"
    cursor.execute(stmt, (total, item['salesregid'], item['dTrainBeginDate'], item['dTrainEndDate']))
    logging.info(f"已更新匯總紀錄: {item['salesregid']} 課程年月: {item['cClassYM']}，新總數: {total}")

def fetch_tasks() -> list[tuple[Any, ...]] | None | list[Any]:
    """从数据库获取待处理任务"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute("""
                    SELECT
                        A.cInsuLicense as salesregid,
                        DATEDIFF(second, '1970-01-01', DATEADD(hour, -8, dTrainBeginDate)) as finish_start_date,
                        DATEDIFF(second, '1970-01-01', DATEADD(hour, -8, DATEADD(day, 1, dTrainEndDate)))-1 as finish_end_date,
                        CONVERT(VARCHAR(10), dTrainBeginDate, 120) AS dTrainBeginDate,
                        CONVERT(VARCHAR(10), dTrainEndDate, 120) AS dTrainEndDate,
                        nTotalComplete,
                        cClassYM,
                        cRegNumber
                    FROM NYDB.AT.InsuExternalTrainingX A
                    JOIN NYDB.AT.vInsuSalesEmpX B
                      ON  B.cEmpIdn = A.cEmpIdn
                      AND B.cWorkingStatus = 'W'
                    WHERE A.cRegNumber IS NOT NULL
                    AND   A.nTotalComplete <> nShouldComplete
                """)
                return cursor.fetchall()
    except Exception as e:
        logging.error(f"獲取任務失敗: {e}")
        return []

def process_single_task(item: Dict, cookie_str: str) -> bool:
    """处理单个任务"""
    try:
        result = sync_data(item, cookie_str)
        time.sleep(random.uniform(1, 3))
        return result
    except Exception as e:
        logging.error(f"任務處理異常: {item['salesregid']} - {e}")
        return False

# --- 主執行程序 (Main Execution) ---
def main():
    """主程序"""
    # 1. 檢查或獲取 Cookie
    cookie_str = get_cookie()
    if not cookie_str:
        logging.info("本地無有效 Cookie，執行登入程序。")
        if not login_and_save_cookie():
            logging.error("登入失敗，程序終止。")
            return
        cookie_str = get_cookie()
        if not cookie_str:
            logging.error("即使登入後也無法獲取 Cookie，程序終止。")
            return

    # 2. 獲取待處理資料
    tasks = fetch_tasks()
    if not tasks:
        logging.info("没有需要處理的資料。")
        return

    # 3. 同步處理資料
    total = len(tasks)
    logging.info(f"開始處理 {total} 條資料")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 使用 lambda 將 cookie_str 傳遞給 process_single_task
        results = list(executor.map(lambda task: process_single_task(task, cookie_str), tasks))
    
    success_count = sum(results)
    logging.info(f"處理完成: 成功 {success_count}/{total} 條")

if __name__ == "__main__":
    main()