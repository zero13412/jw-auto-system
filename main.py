from fastapi import FastAPI, Query, UploadFile, File, Request, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import openpyxl
import gspread
from google.oauth2.service_account import Credentials
import re
import os
import io
import threading
import uuid
import gc
from datetime import timedelta, datetime
import requests
from bs4 import BeautifulSoup

# LINE Bot 官方套件
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, FileMessage

app = FastAPI(title="🚗 杰運汽車內部系統 - 終極安全版")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= LINE Bot 鑰匙設定 =================
LINE_CHANNEL_ACCESS_TOKEN = "Vetc+mW1cmCmkEkXI7GcWpVtqqCkSEDSp/wQuOrQB0SA2GCanyXBmMczQzRW+CK8Obpv2gOMap4rtxRQIa/8/8eqCpdBm/zwozhJndUIEe+NSwPITjCVkPDbKG3usLC/jkh8KlqEkbDoAM8XFYTLRwdB04t89/1O/w1cDnyilFU="
LINE_CHANNEL_SECRET = "ff5426c6ab3102189f8d45f0eca69652"

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ================= Google Sheet 設定 =================
SHEET_ID = "1HWb5u6EGYSHVJHFhmhmsVv4xDgHlQEkdicfXBuFp86w"
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=0"
SIMPLE_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=852175657"

cached_df = None

def get_gspread_client():
    key_path = "/etc/secrets/google_key.json"
    if not os.path.exists(key_path):
        raise Exception("尚未設定 Google API 憑證！")
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(key_path, scopes=scopes)
    return gspread.authorize(creds)

def get_or_create_creds():
    client = get_gspread_client()
    doc = client.open_by_key(SHEET_ID)
    try:
        ws = doc.worksheet("系統設定")
        data = ws.get_all_values()
        user = data[1][1] if len(data) > 1 and len(data[1]) > 1 else "Admin02"
        pwd = data[2][1] if len(data) > 2 and len(data[2]) > 1 else "Eric740625"
        return user, pwd
    except Exception:
        try:
            ws = doc.add_worksheet(title="系統設定", rows="10", cols="5")
            ws.update(values=[["項目", "數值"], ["後台帳號", "Admin02"], ["後台密碼", "Eric740625"]], range_name='A1')
        except: pass
        return "Admin02", "Eric740625"

def update_creds(user, pwd):
    client = get_gspread_client()
    doc = client.open_by_key(SHEET_ID)
    try:
        ws = doc.worksheet("系統設定")
        ws.update(values=[[user], [pwd]], range_name='B2:B3')
    except: pass

def check_permission(user_id, action):
    if not user_id: return False
    try:
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        ws = doc.worksheet("權限管理")
        records = ws.get_all_records()
        for r in records:
            if str(r.get("LINE ID", "")).strip() == user_id:
                if str(r.get("最高管理員", "")).strip().upper() == "V": return True
                return str(r.get(action, "")).strip().upper() == "V"
        return False
    except: return False

def clean_money(val):
    if pd.isna(val): return 0.0
    s = str(val).replace(',', '')
    matches = re.findall(r"(\d+\.?\d*)", s)
    if matches:
        try: 
            v = float(matches[-1])
            if v > 1000: return round(v / 10000, 2)
            return v
        except: return 0.0
    return 0.0

def parse_roc_date(date_val):
    if pd.isna(date_val): return pd.NaT
    s = str(date_val).strip().replace(".", "/").replace("-", "/")
    if not s: return pd.NaT
    try:
        parts = s.split('/')
        if len(parts) == 3:
            year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
            if year < 1911: year += 1911
            return pd.Timestamp(year=year, month=month, day=day)
        return pd.to_datetime(s, errors='coerce')
    except: return pd.NaT

def load_and_clean_data():
    global cached_df
    client = get_gspread_client()
    doc = client.open_by_key(SHEET_ID)
    dfs = []
    try:
        ws_main = doc.worksheet("E車源")
        df_main = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={ws_main.id}")
        df_main['is_sold_sheet'] = False
        dfs.append(df_main)
    except: pass

    if not dfs: df = pd.read_csv(CSV_URL); df['is_sold_sheet'] = False
    else: df = pd.concat(dfs, ignore_index=True)

    df.columns = [str(c).strip() for c in df.columns]
    
    if '採購' not in df.columns: 
        if '採購人' in df.columns: df['採購'] = df['採購人']
        elif '車輛負責人' in df.columns: df['採購'] = df['車輛負責人']
        else: df['採購'] = ""

    df['編號'] = df.apply(lambda r: f"{str(r.get('舊編號','')).replace('.0','')} ({str(r.get('新編號','')).replace('.0','')})" if str(r.get('新編號','')).strip() and str(r.get('舊編號','')).strip() else (str(r.get('新編號','')) or str(r.get('舊編號',''))), axis=1)

    if '網路' in df.columns: df['顯示價格'] = df['網路'].apply(clean_money)
    elif '底價' in df.columns: df['顯示價格'] = df['底價'].apply(clean_money)
    else: df['顯示價格'] = 0.0

    if '廠牌' in df.columns:
        df['廠牌'] = df['廠牌'].apply(lambda b: re.sub(r'[\u4e00-\u9fa5]', '', str(b).split('/')[0]).strip().upper())

    if '車輛位置' in df.columns:
        def clean_loc(loc):
            loc = str(loc)
            if '台北' in loc or '北投' in loc: return '北投店'
            if '桃園' in loc: return '桃園店'
            if '台中' in loc: return '台中店'
            if '高雄' in loc: return '高雄新廠'
            return loc
        df['車輛位置'] = df['車輛位置'].apply(clean_loc)

    def normalize_property(row):
        p, c = str(row.get('產權', '')), str(row.get('公司', ''))
        full = p + c
        if '禾迪' in full: return '禾迪'
        if '展帆' in full: return '展帆'
        if '租車' in full: return '杰租'
        return '杰運' if '杰' in full else '其他'
    df['filter_property'] = df.apply(normalize_property, axis=1)
    
    df['is_sold'] = df.apply(lambda r: '已售' in str(r.get('狀態', '')) or r.get('is_sold_sheet', False), axis=1)
    df['is_cert'] = df['狀態'].apply(lambda x: '取證' in str(x))
    df['is_reserved'] = df.apply(lambda r: '已收訂' in str(r.get('狀態', '')) or '已收訂' in str(r.get('收訂狀態', '')), axis=1)
    
    if '入庫日期' in df.columns: df['入庫_dt'] = df['入庫日期'].apply(parse_roc_date)
    df = df.fillna("")
    cached_df = df
    gc.collect() 
    return df

# ================= 🚀 API 區塊 =================

@app.get("/api/my_permissions")
def get_my_permissions(user_id: str = "", user_name: str = ""):
    if not user_id: return {"status": "error", "permissions": {}}
    try:
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        ws = doc.worksheet("權限管理")
        records = ws.get_all_records()
        for r in records:
            if str(r.get("LINE ID", "")).strip() == user_id:
                return {"status": "success", "permissions": r, "is_new": False}
        ws.append_row([user_name, user_id], value_input_option='USER_ENTERED')
        return {"status": "success", "permissions": {}, "is_new": True}
    except: return {"status": "error", "permissions": {}}

@app.get("/api/check_auth")
def check_auth(user_id: str = "", action: str = ""):
    if not user_id or not action: return {"authorized": False}
    return {"authorized": check_permission(user_id, action)}

@app.get("/api/refresh")
def refresh_data():
    load_and_clean_data()
    return {"message": "資料已更新"}

@app.get("/api/options")
def get_options():
    if cached_df is None: load_and_clean_data()
    brands = sorted([str(x) for x in cached_df['廠牌'].unique() if x])
    locations = sorted([str(x) for x in cached_df['車輛位置'].unique() if x])
    props = sorted([str(x) for x in cached_df['filter_property'].unique() if x and x != "其他"])
    if "其他" in cached_df['filter_property'].unique(): props.append("其他")
    return {"brands": ["全部"] + brands, "locations": ["全部"] + locations, "properties": ["全部"] + props}

@app.get("/api/cars")
def get_cars(brand: str = "全部", location: str = "全部", prop: str = "全部", model: str = "", plate: str = "", person: str = "", min_price: float = 0.0, max_price: float = 99999.0, sort_by: str = "預設", limit: int = 100, hide_no_price: str = "false", hide_sold: str = "false", hide_cert: str = "false", hide_reserved: str = "false"):
    if cached_df is None: load_and_clean_data()
    res = cached_df.copy()
    if brand != "全部": res = res[res['廠牌'] == brand]
    if location != "全部": res = res[res['車輛位置'] == location]
    if prop != "全部": res = res[res['filter_property'] == prop]
    if model: res = res[res['車型'].astype(str).str.contains(model, case=False)]
    if plate: res = res[res['車牌'].astype(str).str.contains(plate, case=False)]
    if person:
        mask = pd.Series(False, index=res.index)
        if '採購' in res.columns: mask = mask | res['採購'].astype(str).str.contains(person, case=False)
        res = res[mask]

    res = res[(res['顯示價格'] >= min_price) & (res['顯示價格'] <= max_price)]
    if hide_no_price == "true": res = res[res['顯示價格'] > 0]
    if hide_sold == "true": res = res[res['is_sold'] == False]
    if hide_cert == "true": res = res[res['is_cert'] == False]
    if hide_reserved == "true": res = res[res['is_reserved'] == False]

    if sort_by == "價格低到高": res = res.sort_values('顯示價格')
    elif sort_by == "價格高到低": res = res.sort_values('顯示價格', ascending=False)
    elif sort_by == "最新入庫": res = res.sort_values('入庫_dt', ascending=False)
    else: res = res.sort_values('年份', ascending=False)

    return {"total": len(res), "data": res.head(limit).to_dict(orient="records")}

# 🚀 【完美升級】：無限換頁自動爬蟲與嚴格的防呆機制
@app.get("/api/sync_car_source")
def sync_car_source_from_backend(user_id: str = "", u: str = "", p: str = ""):
    # 1. 第一關：嚴格檢查「更新車源」權限
    if not check_permission(user_id, "更新車源"):
        return {"status": "error", "message": "⛔ 權限不足！您的帳號未開通「更新車源」功能，請聯繫阿鍇。"}

    try:
        login_user, login_pwd = (u, p) if u and p else get_or_create_creds()
        session = requests.Session()
        login_url = "https://www.jwincar.com.tw/manage/login/index.php"
        data_url = "https://www.jwincar.com.tw/manage/accounting/accounting_car_list.php?stock=all"
        
        session.post(login_url, data={"strID": login_user, "strPW": login_pwd, "Submit": "送出"})
        
        all_cars = []
        headers = []
        page_num = 1
        last_first_row = ""
        
        # 🚀 無限感應換頁：不依賴頁碼文字，直接抓到沒有車為止！
        while True:
            res = session.get(data_url + f"&page={page_num}")
            res.encoding = 'utf-8'
            soup = BeautifulSoup(res.text, "html.parser")
            table = soup.find("table", {"id": "carTable"})
            
            # 如果連第一頁表格都找不到，代表密碼錯誤
            if not table:
                if page_num == 1:
                    return {"status": "need_login", "message": "公司後台密碼已更改，系統無法登入！\n請重新輸入最新的帳號密碼。"}
                break
            
            if u and p and page_num == 1: update_creds(u, p)

            rows = table.find_all("tr")
            if len(rows) <= 1: break # 空白頁結束
            
            # 防死迴圈：如果發現第一台車跟上一頁一模一樣，代表已經超過總頁數，後台卡在最後一頁
            current_first_row = rows[1].text.strip()
            if current_first_row == last_first_row: break
            last_first_row = current_first_row

            if page_num == 1:
                headers = [th.text.replace("⇅", "").strip() for th in rows[0].find_all("th")]
                
            for row in rows[1:]:
                tds = row.find_all("td")
                if not tds: continue
                row_data = []
                for idx, td in enumerate(tds):
                    val = td.text.strip()
                    if val in ["—", "-"]: val = ""
                    if td.has_attr("title"): val = td["title"].strip()
                    if headers and idx < len(headers) and headers[idx] == "狀態":
                        if td.find("span", class_=re.compile(r"sold|已售")) or td.find(string=re.compile(r"已售")): val = "已售"
                        elif td.find("span", class_=re.compile(r"stock|在庫")): val = "在庫"
                        elif td.find("span", class_=re.compile(r"deposit|收訂")) or td.find(string=re.compile(r"已收訂")): val = "已收訂"
                    row_data.append(val)
                all_cars.append(row_data)
                
            page_num += 1
            if page_num > 100: break # 最多抓100頁(3000台)，絕對的安全煞車

        # 🚨 熔斷機制：保護資料庫不被洗白
        if len(all_cars) < 100:
            return {"status": "error", "message": f"🚨 數據異常熔斷！後台只回傳了 {len(all_cars)} 筆。\n為保護您的原始資料庫，系統已自動拒絕寫入。"}

        df_crawled = pd.DataFrame(all_cars, columns=headers)
        if "操作" in df_crawled.columns: df_crawled = df_crawled.drop(columns=["操作"])
        df_crawled = df_crawled.fillna("")

        old_ids = set()
        if cached_df is not None and '新編號' in cached_df.columns:
            old_ids = set(cached_df['新編號'].astype(str).str.strip().tolist())

        new_count = 0
        new_cars_list = []
        if "新編號" in df_crawled.columns:
            for idx, row in df_crawled.iterrows():
                cid = str(row.get("新編號", "")).strip()
                if cid and cid not in old_ids:
                    new_count += 1
                    y = str(row.get("年份", "")).strip()
                    if len(y) == 6 and y.isdigit(): y = f"{y[:4]}年{y[4:]}月"
                    elif len(y) == 4 and y.isdigit(): y = f"{y}年"
                    new_cars_list.append(f"{y} {str(row.get('車型','')).strip()} #{str(row.get('車牌','')).strip()}")
                    old_ids.add(cid)

        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        target_gsheet_main = doc.worksheet("E車源")
        final_headers = list(df_crawled.columns)
        data_to_upload_main = [final_headers] + df_crawled.values.tolist()
        target_gsheet_main.clear()
        target_gsheet_main.update(values=data_to_upload_main, range_name='A1')
        
        status_col_idx = final_headers.index("狀態") if "狀態" in final_headers else -1
        if status_col_idx != -1:
            try: sold_gsheet = doc.worksheet("E車源售出")
            except gspread.exceptions.WorksheetNotFound: sold_gsheet = doc.add_worksheet(title="E車源售出", rows="1000", cols="30")
            try: old_records = sold_gsheet.get_all_records()
            except Exception: old_records = []
            new_records = []
            for row in data_to_upload_main[1:]:
                st = str(row[status_col_idx]).strip()
                if st and st != "在庫":
                    padded = list(row)
                    while len(padded) < len(final_headers): padded.append("")
                    new_records.append(dict(zip(final_headers, padded)))
            if new_records or old_records:
                merged_dict = {}
                for rec in old_records:
                    pk = str(rec.get("車牌", "")).strip() or str(rec.get("新編號", "")).strip() or str(rec.get("車身", "")).strip()
                    if pk: merged_dict[pk] = rec
                for rec in new_records:
                    pk = str(rec.get("車牌", "")).strip() or str(rec.get("新編號", "")).strip() or str(rec.get("車身", "")).strip()
                    if pk: merged_dict[pk] = rec
                    else: merged_dict[str(uuid.uuid4())] = rec
                sold_headers = list(final_headers)
                for rec in merged_dict.values():
                    for k in rec.keys():
                        if k not in sold_headers and str(k).strip(): sold_headers.append(k)
                final_sold_data = [sold_headers]
                for rec in merged_dict.values(): final_sold_data.append([str(rec.get(h, "")) for h in sold_headers])
                sold_gsheet.clear()
                sold_gsheet.update(values=final_sold_data, range_name='A1')
                
        load_and_clean_data()
        
        msg = f"🤖 更新成功！共抓取 {len(all_cars)} 筆車源。"
        if new_count > 0:
            msg += f"\n✨ 自動發現 {new_count} 台新車：\n" + "\n".join(new_cars_list[:10])
        return {"status": "success", "message": msg}

    except Exception as e:
        return {"status": "error", "message": f"爬蟲發生錯誤：{str(e)}"}
    finally:
        gc.collect()

@app.get("/api/search_plate")
def search_plate(plate: str):
    if cached_df is None: load_and_clean_data()
    res = cached_df.copy()
    if '車牌' in res.columns:
        target_plate = plate.strip().upper()
        res['clean_plate'] = res['車牌'].astype(str).str.replace(" ", "").str.upper()
        matches = res[res['clean_plate'].str.contains(target_plate, na=False)]
        if len(matches) > 0:
            car_data = matches.iloc[0].to_dict()
            year_val = str(car_data.get('年份', ''))
            match = re.search(r'\d{4}', year_val)
            car_data['clean_year'] = match.group(0) if match else year_val.replace('.0', '')
            return {"status": "success", "data": car_data}
    return {"status": "error", "message": "查無此車"}

@app.get("/api/simple_data")
def get_simple_data():
    try:
        df_simple = pd.read_csv(SIMPLE_CSV_URL, header=3)
        df_simple = df_simple.dropna(how='all')
        new_columns = []
        empty_count = 0
        for c in df_simple.columns:
            if "Unnamed" in str(c) or str(c).strip() == "":
                empty_count += 1
                new_columns.append(f"__未命名_{empty_count}__")
            else: new_columns.append(str(c).strip())
        df_simple.columns = new_columns
        df_simple = df_simple.dropna(axis=1, how='all')
        df_simple = df_simple.fillna("")
        gc.collect() 
        return {"status": "success", "data": df_simple.to_dict(orient="records")}
    except Exception as e: return {"status": "error", "message": f"讀取失敗：{str(e)}"}

@app.get("/api/customers")
def get_customers():
    try:
        client = get_gspread_client()
        sheet = client.open_by_key(SHEET_ID).worksheet("客資紀錄")
        raw_values = sheet.get_all_values()
        if not raw_values or len(raw_values) < 2: return {"status": "success", "data": []}
        headers = [str(h).strip() for h in raw_values[0]]
        records = []
        for row in raw_values[1:]:
            row_data = [str(cell).strip().lstrip("'") for cell in row]
            while len(row_data) < len(headers): row_data.append("")
            records.append(dict(zip(headers, row_data)))
        return {"status": "success", "data": list(reversed(records))}
    except Exception as e: return {"status": "error", "message": str(e)}

@app.post("/api/customers")
async def add_customer(request: Request):
    try:
        data = await request.json()
        tw_time = datetime.utcnow() + timedelta(hours=8)
        date_str = tw_time.strftime("%Y/%m/%d %H:%M")
        phone_str = str(data.get("phone", "")).strip()
        if phone_str.startswith("0"): phone_str = f"'{phone_str}"
        row_data = [date_str, data.get("name", ""), phone_str, data.get("needs", ""), data.get("memo", "")]
        client = get_gspread_client()
        sheet = client.open_by_key(SHEET_ID).worksheet("客資紀錄")
        sheet.append_row(row_data, value_input_option='USER_ENTERED')
        return {"status": "success", "message": "客資已新增"}
    except Exception as e: return {"status": "error", "message": str(e)}

@app.post("/api/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    # 保持檔案上傳邏輯精簡
    return {"status": "error", "message": "請使用網頁上傳或洽管理員"}

# ================= 🚀 LINE 機器人 =================
@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()
    
    if text == "我的ID" or text.lower() == "my id":
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"👤 您的 LINE ID 為：\n{user_id}"))
        return

    if text in ["更新車源", "抓取車源"]:
        if not check_permission(user_id, "更新車源"):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⛔ 抱歉，您沒有執行「更新車源」的權限。"))
            return
            
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="🤖 身份確認！正在連線後台抓取車源..."))
        def run_task():
            try:
                res = sync_car_source_from_backend(user_id=user_id)
                if res.get("status") == "need_login":
                    line_bot_api.push_message(user_id, TextSendMessage(text="🚨 公司後台密碼已更改，請前往網頁版輸入新密碼！"))
                else:
                    line_bot_api.push_message(user_id, TextSendMessage(text=res["message"]))
            except Exception as e:
                line_bot_api.push_message(user_id, TextSendMessage(text=f"❌ 發生錯誤：{str(e)}"))
        threading.Thread(target=run_task).start()
        return

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="🤖 您好！我是自動小幫手。\n\n▶️ 【車源更新】請說：「更新車源」\n▶️ 【我的權限】請說：「我的ID」"))

@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    try: handler.handle(body.decode("utf-8"), signature)
    except InvalidSignatureError: raise HTTPException(status_code=400)
    return "OK"

@app.get("/")
def serve_home(): return FileResponse("index.html")
@app.head("/")
@app.get("/ping")
def ping(): return {"status": "ok"}
@app.get("/{path}")
def serve_pages(path: str):
    if os.path.exists(f"{path}.html"): return FileResponse(f"{path}.html")
    return FileResponse("index.html")
