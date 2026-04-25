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

app = FastAPI(title="🚗 杰運汽車內部系統 - 安全加強版")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= LINE Bot 設定 =================
LINE_CHANNEL_ACCESS_TOKEN = "Vetc+mW1cmCmkEkXI7GcWpVtqqCkSEDSp/wQuOrQB0SA2GCanyXBmMczQzRW+CK8Obpv2gOMap4rtxRQIa/8/8eqCpdBm/zwozhJndUIEe+NSwPITjCVkPDbKG3usLC/jkh8KlqEkbDoAM8XFYTLRwdB04t89/1O/w1cDnyilFU="
LINE_CHANNEL_SECRET = "ff5426c6ab3102189f8d45f0eca69652"

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

# 🛡️ 權限檢查核心邏輯
def check_permission(user_id, action):
    if not user_id: return False
    try:
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        ws = doc.worksheet("權限管理")
        records = ws.get_all_records()
        for r in records:
            if str(r.get("LINE ID", "")).strip() == user_id:
                # 最高管理員萬能
                if str(r.get("最高管理員", "")).strip().upper() == "V": return True
                # 檢查指定功能
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

@app.get("/api/cars")
def get_cars(brand: str = "全部", location: str = "全部", prop: str = "全部", model: str = "", plate: str = "", person: str = "", min_price: float = 0.0, max_price: float = 99999.0, sort_by: str = "預設", limit: int = 100, hide_no_price: str = "false", hide_sold: str = "false", hide_cert: str = "false", hide_reserved: str = "false"):
    if cached_df is None: load_and_clean_data()
    res = cached_df.copy()
    if brand != "全部": res = res[res['廠牌'] == brand]
    if location != "全部": res = res[res['車輛位置'] == location]
    if prop != "全部": res = res[res['filter_property'] == prop]
    if model: res = res[res['車型'].astype(str).str.contains(model, case=False)]
    if plate: res = res[res['車牌'].astype(str).str.contains(plate, case=False)]
    if person: res = res[res['採購'].astype(str).str.contains(person, case=False)]
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

# 🚀 【安全性升級】：更新車源 API 加入權限檢查與熔斷機制
@app.get("/api/sync_car_source")
def sync_car_source_from_backend(user_id: str = "", u: str = "", p: str = ""):
    # 1. 檢查呼叫者的權限
    if not check_permission(user_id, "更新車源"):
        return {"status": "error", "message": "⛔ 權限不足！您的帳號未開通「更新車源」功能。"}

    try:
        login_user, login_pwd = (u, p) if u and p else get_or_create_creds()
        session = requests.Session()
        res = session.post("https://www.jwincar.com.tw/manage/login/index.php", data={"strID": login_user, "strPW": login_pwd, "Submit": "送出"})
        
        # 抓取第一頁與分析總頁數
        res = session.get("https://www.jwincar.com.tw/manage/accounting/accounting_car_list.php?stock=all")
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.find("table", {"id": "carTable"})
        
        if not table: return {"status": "need_login", "message": "公司後台密碼已更改，系統無法登入！"}
        if u and p: update_creds(u, p)

        # 解析總頁數
        total_pages = 1
        page_info = soup.find(string=re.compile(r"第 \d+ / \d+ 頁"))
        if page_info:
            match = re.search(r"/ (\d+) 頁", page_info)
            if match: total_pages = int(match.group(1))

        all_cars_data = []
        headers = [th.text.replace("⇅", "").strip() for th in table.find_all("tr")[0].find_all("th")]

        for page in range(1, total_pages + 1):
            if page > 1:
                res = session.get(f"https://www.jwincar.com.tw/manage/accounting/accounting_car_list.php?stock=all&page={page}")
                res.encoding = 'utf-8'
                soup = BeautifulSoup(res.text, "html.parser")
                table = soup.find("table", {"id": "carTable"})
            
            for row in table.find_all("tr")[1:]:
                tds = row.find_all("td")
                if tds: all_cars_data.append([td.text.strip() for td in tds])

        # 🚨 【熔斷機制】：如果抓到數量低於 100 台 (預期 700+)，判定為異常，拒絕更新！
        if len(all_cars_data) < 100:
            return {"status": "error", "message": f"🚨 數據異常！後台只回傳了 {len(all_cars_data)} 筆，為保護原始資料，系統已自動拒絕更新。請稍後再試或聯繫阿鍇。"}

        # 成功後才寫入 Google Sheet
        df_crawled = pd.DataFrame(all_cars_data, columns=headers)
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        ws = doc.worksheet("E車源")
        ws.clear()
        ws.update(values=[df_crawled.columns.tolist()] + df_crawled.values.tolist(), range_name='A1')
        load_and_clean_data()
        return {"status": "success", "message": f"🤖 同步成功！共更新 {len(all_cars_data)} 筆車源。"}
    except Exception as e: return {"status": "error", "message": f"執行錯誤：{str(e)}"}

@app.get("/api/search_plate")
def search_plate(plate: str):
    if cached_df is None: load_and_clean_data()
    target = plate.strip().upper()
    matches = cached_df[cached_df['車牌'].astype(str).str.replace(" ","").str.contains(target)]
    if not matches.empty:
        d = matches.iloc[0].to_dict()
        d['clean_year'] = re.search(r'\d{4}', str(d.get('年份',''))).group(0) if re.search(r'\d{4}', str(d.get('年份',''))) else ""
        return {"status": "success", "data": d}
    return {"status": "error", "message": "查無此車"}

@app.get("/api/simple_data")
def get_simple_data():
    try:
        df = pd.read_csv(SIMPLE_CSV_URL, header=3).dropna(how='all').fillna("")
        return {"status": "success", "data": df.to_dict(orient="records")}
    except Exception as e: return {"status": "error", "message": str(e)}

@app.get("/api/options")
def get_options():
    if cached_df is None: load_and_clean_data()
    brands = sorted([str(x) for x in cached_df['廠牌'].unique() if x])
    locations = sorted([str(x) for x in cached_df['車輛位置'].unique() if x])
    props = sorted([str(x) for x in cached_df['filter_property'].unique() if x])
    return {"brands": ["全部"] + brands, "locations": ["全部"] + locations, "properties": ["全部"] + props}

@app.get("/api/refresh")
def refresh_api():
    load_and_clean_data()
    return {"message": "ok"}

@app.get("/")
def serve_home(): return FileResponse("index.html")
@app.get("/{path}")
def serve_pages(path: str):
    if os.path.exists(f"{path}.html"): return FileResponse(f"{path}.html")
    return FileResponse("index.html")
