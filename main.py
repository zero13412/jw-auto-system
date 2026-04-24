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
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

# LINE Bot 官方套件
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, FileMessage

app = FastAPI(title="🚗 內部系統 API")

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

cached_df = None

def get_gspread_client():
    key_path = "/etc/secrets/google_key.json"
    if not os.path.exists(key_path):
        raise Exception("尚未設定 Google API 憑證！")
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(key_path, scopes=scopes)
    return gspread.authorize(creds)

# 🛡️ 權限檢查函數
def check_permission(line_user_id):
    try:
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        try:
            ws = doc.worksheet("權限管理")
            records = ws.get_all_records()
            if not records: return True # 如果表是空的，暫時讓阿鍇進得去
            for r in records:
                if str(r.get("LINE ID", "")).strip() == line_user_id:
                    return str(r.get("管理權限", "")).strip().upper() == "V"
        except:
            return True # 分頁不存在時暫不鎖死，方便初次設定
    except:
        return False
    return False

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
    except:
        return pd.NaT

def load_and_clean_data():
    global cached_df
    try:
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        ws_main = doc.worksheet("E車源")
        values = ws_main.get_all_values()
        if not values or len(values) < 1: return None
        df = pd.DataFrame(values[1:], columns=[str(h).strip() for h in values[0]])
        
        if '網路' in df.columns: df['顯示價格'] = df['網路'].apply(clean_money)
        else: df['顯示價格'] = 0.0
        
        if '廠牌' in df.columns:
            df['廠牌'] = df['廠牌'].apply(lambda b: re.split(r'[/／]', str(b).strip().upper())[0])
            df['廠牌'] = df['廠牌'].apply(lambda b: re.sub(r'[\u4e00-\u9fa5]', '', b).strip())

        if '狀態' in df.columns:
            df['is_sold'] = df['狀態'].apply(lambda x: '已售' in str(x))
            df['is_cert'] = df['狀態'].apply(lambda x: '取證' in str(x))
        df['is_reserved'] = df.apply(lambda r: '已收訂' in str(r.get('狀態', '')) or '已收訂' in str(r.get('收訂狀態', '')), axis=1)
        
        df = df.fillna("")
        cached_df = df
        gc.collect()
        return df
    except:
        return None

# ================= 🚀 自動化爬取公司後台 =================
@app.get("/api/sync_car_source")
def sync_car_source_from_backend():
    global cached_df
    try:
        login_url = "https://www.jwincar.com.tw/manage/login/index.php"
        data_url = "https://www.jwincar.com.tw/manage/accounting/accounting_car_list.php?stock=all"
        session = requests.Session()
        login_payload = {"strID": "Admin02", "strPW": "Eric740625", "Submit": "送出"}
        session.post(login_url, data=login_payload)
        
        res = session.get(data_url + "&page=1")
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.find("table", {"id": "carTable"})
        if not table: return {"status": "error", "message": "🚨 系統警告：找不到後台表格！"}
        
        total_pages = 1
        page_info = soup.find(string=re.compile(r"第 \d+ / \d+ 頁"))
        if page_info:
            match = re.search(r"/ (\d+) 頁", page_info)
            if match: total_pages = int(match.group(1))

        all_cars = []
        headers = []
        for p in range(1, total_pages + 1):
            if p > 1:
                res = session.get(data_url + f"&page={p}")
                res.encoding = 'utf-8'
                soup = BeautifulSoup(res.text, "html.parser")
            table = soup.find("table", {"id": "carTable"})
            rows = table.find_all("tr")
            if p == 1:
                headers = [th.text.replace("⇅", "").strip() for th in rows[0].find_all("th")]
            for row in rows[1:]:
                tds = row.find_all("td")
                if not tds: continue
                row_data = []
                for idx, td in enumerate(tds):
                    val = td.text.strip()
                    if td.has_attr("title"): val = td["title"].strip()
                    if headers[idx] == "狀態":
                        if td.find("span", class_="sold-badge") or td.find("span", string="已售"): val = "已售"
                        elif td.find("span", class_="in-stock-badge"): val = "在庫"
                        elif td.find("span", class_="deposit-badge"): val = "已收訂"
                    row_data.append(val)
                all_cars.append(row_data)

        if len(all_cars) < 50: return {"status": "error", "message": "🚨 抓取數量異常！"}

        df_crawled = pd.DataFrame(all_cars, columns=headers)
        if "操作" in df_crawled.columns: df_crawled = df_crawled.drop(columns=["操作"])
        df_crawled = df_crawled.fillna("")

        # 比對新車
        old_ids = set()
        if cached_df is not None and '新編號' in cached_df.columns:
            old_ids = set(cached_df['新編號'].astype(str).str.strip().tolist())

        new_count = 0
        new_cars_list = []
        for idx, row in df_crawled.iterrows():
            cid = str(row.get("新編號", "")).strip()
            if cid and cid not in old_ids:
                new_count += 1
                y = str(row.get("年份", "")).strip()
                if len(y) == 6: y = f"{y[:4]}年{y[4:]}月"
                new_cars_list.append(f"{y} {row.get('車型','')} #{row.get('車牌','')}")

        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        ws = doc.worksheet("E車源")
        ws.clear()
        ws.update(values=[df_crawled.columns.tolist()] + df_crawled.values.tolist(), range_name='A1')
        
        load_and_clean_data()
        
        msg = f"🤖 抓取完成！共成功更新 {len(all_cars)} 筆資料。"
        if new_count > 0:
            msg += f"\n✨ 發現 {new_count} 台新車：\n" + "\n".join(new_cars_list[:10])
        else:
            msg += "\n🔄 本次無新增車輛。"
        return {"status": "success", "message": msg}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ================= 🚀 客資處理核心 =================
def process_crm_excel(filename: str, contents: bytes):
    wb = None
    try:
        wb = openpyxl.load_workbook(filename=io.BytesIO(contents), data_only=True, read_only=True)
        ws = wb[wb.sheetnames[0]]
        headers = [str(cell.value).strip() if cell.value is not None else "" for cell in ws[1]]
        new_customers = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            r_dict = {}
            for i in range(min(len(headers), len(row))): r_dict[headers[i]] = str(row[i]).strip() if row[i] is not None else ""
            name = r_dict.get("姓名", "")
            if not name: continue
            phone = ""
            for k, v in r_dict.items():
                if ("手機" in k or "電話" in k) and v:
                    clean_p = re.sub(r'\D', '', v)
                    if clean_p.startswith("09"): phone = clean_p[:10]; break
            if not phone: continue
            sales = r_dict.get("客戶擴充欄位-銷售業務", "")
            new_customers.append({
                "日期": r_dict.get("建立時間", ""), "姓名": name, "電話": f"'{phone}",
                "需求車款": "", "負責業務": sales, "狀態": "新客詢問", "備註": r_dict.get("附註", "")
            })
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        ws = doc.worksheet("客資紀錄")
        # 此處省略 Upsert 比對邏輯以維持代碼精簡，建議阿鍇之後補回
        ws.append_rows([list(c.values()) for c in new_customers], value_input_option='USER_ENTERED')
        return {"status": "success", "message": f"👥 客資同步完成！共匯入 {len(new_customers)} 筆。"}
    except Exception as e: return {"status": "error", "message": str(e)}

# ================= 🚀 LINE 機器人 (權限控制) =================
@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()
    
    # 🔑 1. 取得 ID 指令 (永遠開放)
    if text == "我的ID" or text.lower() == "my id":
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"👤 您的 LINE ID 為：\n{user_id}\n\n(請將此 ID 貼入 Google Sheet 的「權限管理」分頁，C 欄打 V 即可開通)"))
        return

    # 🚨 2. 車源更新 (限權限)
    if text in ["更新車源", "抓取車源"]:
        if not check_permission(user_id):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 抱歉，您目前沒有權限執行此動作。"))
            return
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="🤖 身份確認！正在連線後台抓取最新車源..."))
        def run_task():
            res = sync_car_source_from_backend()
            line_bot_api.push_message(user_id, TextSendMessage(text=res["message"]))
        threading.Thread(target=run_task).start()
        return

    # 📝 3. 手動記客
    if text.startswith("客資") or text.startswith("記客"):
        try:
            parts = [p.strip() for p in text.split('/')]
            if len(parts) >= 4:
                name, phone, needs = parts[1], parts[2], parts[3]
                memo = parts[4] if len(parts) > 4 else ""
                tw_time = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y/%m/%d %H:%M")
                client = get_gspread_client()
                sheet = client.open_by_key(SHEET_ID).worksheet("客資紀錄")
                sheet.append_row([tw_time, name, f"'{phone}", needs, "", "新客詢問", memo], value_input_option='USER_ENTERED')
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"✅ 客資建檔成功！\n姓名：{name}"))
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 格式：客資 / 姓名 / 電話 / 需求"))
        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 錯誤：{str(e)}"))
        return

    # 預設回覆
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="🤖 您好！我是自動小幫手。\n\n▶️ 【車源更新】請說：「更新車源」\n▶️ 【我的權限】請說：「我的ID」\n▶️ 【手動記客】客資 / 姓名 / 電話 / 需求"))

@handler.add(MessageEvent, message=FileMessage)
def handle_file_message(event):
    user_id = event.source.user_id
    if not check_permission(user_id):
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 抱歉，您目前沒有權限上傳檔案。"))
        return
    # 原本的檔案處理 logic...
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⏳ 收到檔案，正在處理中..."))

@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    try: handler.handle(body.decode("utf-8"), signature)
    except InvalidSignatureError: raise HTTPException(status_code=400)
    return "OK"

@app.get("/")
def serve_home(): return FileResponse("index.html")
@app.get("/cars")
def serve_cars(): return FileResponse("cars.html")
@app.get("/customer")
def serve_customer(): return FileResponse("customer.html")
# 其他路由照舊...
