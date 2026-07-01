from fastapi import FastAPI, Query, UploadFile, File, Request, HTTPException
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import openpyxl
from openpyxl.styles import Alignment, Font
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
from urllib.parse import quote

# LINE Bot 官方套件
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, FileMessage

app = FastAPI(title="🚗 杰運汽車新竹店阿鍇專用 - 內部系統")

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
view_api_session = None

KNOWN_MAKES = [
    "TOYOTA", "HONDA", "BENZ", "BMW", "AUDI", "LEXUS", "VOLVO", "VW", "MAZDA", 
    "NISSAN", "FORD", "PORSCHE", "MG", "SKODA", "MINI", "KIA", "SUZUKI", 
    "MITSUBISHI", "LUXGEN", "LAND ROVER", "JAGUAR", "SUBARU", "TESLA", 
    "MASERATI", "FERRARI", "LAMBORGHINI", "BENTLEY", "ROLLS-ROYCE"
]

def get_gspread_client():
    key_path = "/etc/secrets/google_key.json"
    if not os.path.exists(key_path):
        raise Exception("尚未設定 Google API 憑證！")
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(key_path, scopes=scopes)
    return gspread.authorize(creds)

# 🔐 系統智慧保險箱
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

# 🛡️ 權限檢查核心
def check_permission(user_id, action):
    if not user_id: return False
    try:
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        ws = doc.worksheet("權限管理")
        records = ws.get_all_records()
        for r in records:
            if str(r.get("LINE ID", "")).strip() == str(user_id).strip():
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
        if "車牌" not in df_main.columns:
            for idx, row in df_main.iterrows():
                vals = [str(x).strip() for x in row.values]
                if "車牌" in vals and ("廠牌" in vals or "品牌" in vals or "車型" in vals):
                    df_main.columns = vals
                    df_main = df_main.iloc[idx+1:].reset_index(drop=True)
                    break
        df_main['is_sold_sheet'] = False
        dfs.append(df_main)
    except: pass

    if not dfs: 
        df = pd.read_csv(CSV_URL)
        if "車牌" not in df.columns:
            for idx, row in df.iterrows():
                vals = [str(x).strip() for x in row.values]
                if "車牌" in vals:
                    df.columns = vals
                    df = df.iloc[idx+1:].reset_index(drop=True)
                    break
        df['is_sold_sheet'] = False
    else: 
        df = pd.concat(dfs, ignore_index=True)

    df.columns = [str(c).strip() for c in df.columns]
    
    if '採購' not in df.columns: 
        if '採購人' in df.columns: df['採購'] = df['採購人']
        elif '車輛負責人' in df.columns: df['採購'] = df['車輛負責人']
        elif '負責人' in df.columns: df['採購'] = df['負責人']
        else: df['採購'] = ""

    df['編號'] = df.apply(lambda r: f"{str(r.get('舊編號','')).replace('.0','')} ({str(r.get('新編號','')).replace('.0','')})" if str(r.get('新編號','')).strip() and str(r.get('舊編號','')).strip() else (str(r.get('新編號','')) or str(r.get('舊編號',''))), axis=1)

    if '網路' in df.columns: df['顯示價格'] = df['網路'].apply(clean_money)
    elif '售價' in df.columns: df['顯示價格'] = df['售價'].apply(clean_money)
    elif '價格' in df.columns: df['顯示價格'] = df['價格'].apply(clean_money)
    elif '底價' in df.columns: df['顯示價格'] = df['底價'].apply(clean_money)
    else: df['顯示價格'] = 0.0

    if '廠牌' in df.columns:
        df['廠牌'] = df['廠牌'].apply(lambda b: re.sub(r'[\u4e00-\u9fa5]', '', str(b).split('/')[0]).strip().upper())
    elif '品牌' in df.columns:
        df['廠牌'] = df['品牌'].apply(lambda b: re.sub(r'[\u4e00-\u9fa5]', '', str(b).split('/')[0]).strip().upper())

    if '年份' in df.columns: df['年份'] = df['年份'].astype(str)

    if '里程' in df.columns:
        def clean_mileage(m):
            if pd.isna(m): return ""
            m_str = str(m).replace(',', '').strip()
            if m_str.lower() == 'nan': return ""
            if m_str.endswith('.0'): return m_str[:-2]
            return m_str
        df['里程'] = df['里程'].apply(clean_mileage)

    if '車輛位置' in df.columns:
        def clean_loc(loc):
            loc = str(loc).strip()
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
        if '租車' in full or '杰租' in full: return '杰租'
        return '杰運' if '杰' in full else '其他'
    df['filter_property'] = df.apply(normalize_property, axis=1)
    
    if '狀態' in df.columns:
        df['is_sold'] = df.apply(lambda r: True if '已售' in str(r.get('狀態', '')) or r.get('is_sold_sheet') else False, axis=1)
        df['is_cert'] = df['狀態'].apply(lambda x: True if '取證' in str(x) else False)
    else:
        df['is_sold'] = df.get('is_sold_sheet', False)
        df['is_cert'] = False
        
    df['is_reserved'] = df.apply(lambda r: True if '已收訂' in str(r.get('狀態', '')) or '已收訂' in str(r.get('收訂狀態', '')) else False, axis=1)
    if '入庫日期' in df.columns: df['入庫_dt'] = df['入庫日期'].apply(parse_roc_date)
    elif '入庫日' in df.columns: df['入庫_dt'] = df['入庫日'].apply(parse_roc_date)
    
    df = df.fillna("")
    cached_df = df
    gc.collect() 
    return df

# ================= 🚀 Excel/PDF 解析模組 =================
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
                    if clean_p.startswith("09") and len(clean_p) >= 10:
                        phone = clean_p[:10]
                        break
            if not phone: continue 
            date_val = r_dict.get("生效日", "") or r_dict.get("建立時間", "")
            memo = r_dict.get("附註", "")
            tags = r_dict.get("標籤", "")
            status = "新客詢問"
            if "成交" in tags: status = "已成交"
            elif "收訂" in tags: status = "Accent已收訂" if "Accent" in tags else "已收訂"
            elif "戰敗" in tags or "放棄" in tags or "暫緩" in tags: status = "戰敗"
            elif "賞車" in tags or "看車" in tags: status = "安排賞車"
            sales = r_dict.get("客戶擴充欄位-銷售業務", "")
            if not sales:
                for k, v in r_dict.items():
                    if ("業務" in k or "負責" in k) and v and "@" not in v: sales = v; break
            needs = ""
            for k, v in r_dict.items():
                if ("車" in k or "需求" in k) and "車牌" not in k and "車身" not in k and v: needs = v; break
            new_customers.append({"日期": date_val, "姓名": name, "電話": f"'{phone}", "需求車款": needs, "負責業務": sales, "狀態": status, "備註": memo})
            
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        try:
            sheet = doc.worksheet("客資紀錄")
            raw_values = sheet.get_all_values()
            old_records = []
            if raw_values and len(raw_values) > 1:
                hdrs = [str(h).strip() for h in raw_values[0]]
                for r in raw_values[1:]:
                    padded = list(r)
                    while len(padded) < len(hdrs): padded.append("")
                    old_records.append(dict(zip(hdrs, padded)))
        except:
            sheet = doc.add_worksheet("客資紀錄", 1000, 10)
            old_records = []
        merged_dict = {}
        for rec in old_records:
            p = str(rec.get("電話", "")).replace("'", "").strip()
            if p: merged_dict[p] = rec
        update_count, add_count = 0, 0
        for nc in new_customers:
            p = nc["電話"].replace("'", "")
            if p in merged_dict:
                update_count += 1
                existing = merged_dict[p]
                if nc["需求車款"]: existing["需求車款"] = nc["需求車款"]
                if nc["負責業務"]: existing["負責業務"] = nc["負責業務"] 
                if nc["狀態"] and nc["狀態"] != "新客詢問": existing["狀態"] = nc["狀態"]
                if nc["備註"]: existing["備註"] = nc["備註"]
                merged_dict[p] = existing
            else:
                add_count += 1
                merged_dict[p] = nc
        headers_crm = ["日期", "姓名", "電話", "需求車款", "負責業務", "狀態", "備註"]
        final_data = [headers_crm]
        for p, rec in merged_dict.items(): final_data.append([str(rec.get(h, "")) for h in headers_crm])
        sheet.clear()
        sheet.update(values=final_data, range_name="A1")
        return {"status": "success", "message": f"👥 客資同步完成！\n本次新增 {add_count} 筆，更新 {update_count} 筆。"}
    except Exception as e: return {"status": "error", "message": f"客資處理失敗：{str(e)}"}
    finally:
        if wb: wb.close()
        gc.collect()

def process_pdf_file(filename: str, contents: bytes):
    try: import pdfplumber
    except ImportError: return {"status": "error", "message": "缺少 pdfplumber 套件"}
    try:
        target_tab_name = "新竹車源" if "新竹" in filename else "E車源"
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        try: target_gsheet = doc.worksheet(target_tab_name)
        except gspread.exceptions.WorksheetNotFound: return {"status": "error", "message": f"找不到 {target_tab_name}"}
        all_rows, headers = [], []
        with pdfplumber.open(io.BytesIO(contents)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    for row in table:
                        cleaned_row = [str(cell).replace('\n', ' ').strip() if cell is not None else "" for cell in row]
                        if not any(cleaned_row): continue
                        if not headers and any(kw in str(cleaned_row) for kw in ["車牌", "廠牌", "品牌", "年份", "新編號"]):
                            headers = cleaned_row; continue
                        if headers: all_rows.append(cleaned_row)
        if not headers: return {"status": "error", "message": "無法解析 PDF 表格"}
        if "狀態" not in headers: headers.append("狀態")
        status_col_idx = headers.index("狀態")
        data_to_upload = [headers]
        for row in all_rows:
            while len(row) <= status_col_idx: row.append("")
            row[status_col_idx] = "在庫"; data_to_upload.append(row)
        target_gsheet.clear()
        target_gsheet.update(values=[[str(cell) for cell in row] for row in data_to_upload], range_name='A1')
        status_counts = {}
        for row in data_to_upload[1:]:
            val = str(row[status_col_idx]).strip() or "在庫"
            status_counts[val] = status_counts.get(val, 0) + 1
        load_and_clean_data()
        return {"status": "success", "message": f"📄 PDF 解析成功！共更新 {len(data_to_upload)-1} 筆車輛！"}
    except Exception as e: return {"status": "error", "message": f"PDF 處理失敗：{str(e)}"}
    finally: gc.collect()

def process_excel_file(filename: str, contents: bytes):
    wb = None
    try:
        target_tab_name = "新竹車源" if "新竹" in filename else "E車源"
        wb = openpyxl.load_workbook(filename=io.BytesIO(contents), data_only=True)
        ws_main = wb[wb.sheetnames[0]]
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        try: target_gsheet_main = doc.worksheet(target_tab_name)
        except gspread.exceptions.WorksheetNotFound: return {"status": "error", "message": f"找不到分頁「{target_tab_name}」"}

        data_to_upload_main = []
        color_requests_main = []

        if target_tab_name == "新竹車源":
            color_requests_main = [{"repeatCell": {"range": { "sheetId": target_gsheet_main.id }, "cell": {"userEnteredFormat": {"backgroundColor": { "red": 1.0, "green": 1.0, "blue": 1.0 }}}, "fields": "userEnteredFormat.backgroundColor"}}]
            header_row_idx = -1
            headers_main = []
            for i, row in enumerate(ws_main.iter_rows(values_only=True)):
                vals = [str(v).strip() if v is not None else "" for v in row]
                if "車牌" in vals and ("車型" in vals or "品牌" in vals or "廠牌" in vals):
                    header_row_idx = i; headers_main = vals; break
            if header_row_idx == -1: header_row_idx = 0; headers_main = [str(cell.value).strip() if cell.value is not None else "" for cell in ws_main[1]]

            old_keys = set()
            is_excel_initial = False
            try:
                old_values = target_gsheet_main.get_all_values()
                if old_values and len(old_values) > header_row_idx:
                    old_hdrs = [str(x).strip() for x in old_values[header_row_idx]]
                    p_idx = old_hdrs.index("車牌") if "車牌" in old_hdrs else -1
                    v_idx = old_hdrs.index("車身") if "車身" in old_hdrs else -1
                    n_idx = old_hdrs.index("新編號") if "新編號" in old_hdrs else -1
                    for row in old_values[header_row_idx+1:]:
                        key = ""
                        if n_idx != -1 and len(row) > n_idx and str(row[n_idx]).strip(): key = str(row[n_idx]).strip()
                        elif p_idx != -1 and len(row) > p_idx and str(row[p_idx]).strip(): key = str(row[p_idx]).strip()
                        elif v_idx != -1 and len(row) > v_idx and str(row[v_idx]).strip(): key = str(row[v_idx]).strip()
                        if key and "車款" not in key and "欄" not in key: old_keys.add(str(key).replace('.0', '').strip())
                else: is_excel_initial = True
            except: is_excel_initial = True

            col_model = headers_main.index("車型") if "車型" in headers_main else -1
            plate_idx = headers_main.index("車牌") if "車牌" in headers_main else -1
            vin_idx = headers_main.index("車身") if "車身" in headers_main else -1
            no_idx = headers_main.index("新編號") if "新編號" in headers_main else -1
            year_idx = headers_main.index("年份") if "年份" in headers_main else -1
            col_color = headers_main.index("顏色") if "顏色" in headers_main else -1
            if "收訂狀態" not in headers_main: headers_main.append("收訂狀態")
            status_idx = headers_main.index("收訂狀態")
            new_count, new_cars_list = 0, []

            for r_idx, row in enumerate(ws_main.iter_rows()):
                row_values = [cell.value if cell.value is not None else "" for cell in row]
                while len(row_values) < len(headers_main): row_values.append("")
                if r_idx < header_row_idx or r_idx == header_row_idx or not any(str(v).strip() for v in row_values):
                    data_to_upload_main.append(row_values); continue

                while len(row_values) <= status_idx: row_values.append("")
                n_val = str(row_values[no_idx]).replace('.0', '').strip() if no_idx != -1 else ""
                p_val = str(row_values[plate_idx]).replace('.0', '').strip() if plate_idx != -1 else ""
                v_val = str(row_values[vin_idx]).replace('.0', '').strip() if vin_idx != -1 else ""
                row_key = n_val if n_val else (p_val if p_val else v_val)
                is_subheader = "車款" in p_val or "車輛數" in str(row_values[0]) or "欄1" in str(row_values)
                if row_key and not is_subheader and not is_excel_initial and row_key not in old_keys:
                    new_count += 1
                    y_val = str(row_values[year_idx]).replace('.0', '').strip() if year_idx != -1 else ""
                    if len(y_val) == 6 and y_val.isdigit(): y_val = f"{y_val[:4]}年{y_val[4:]}月"
                    elif len(y_val) == 4 and y_val.isdigit(): y_val = f"{y_val}年"
                    m_val = str(row_values[col_model]).strip() if col_model != -1 else ""
                    c_val = str(row_values[col_color]).strip() if col_color != -1 else ""
                    new_cars_list.append(f"{y_val} {m_val} {c_val} #{p_val if p_val else '(無車牌)'}")
                    old_keys.add(row_key)
                if not is_subheader:
                    col_i_val = str(row_values[8]).strip() if len(row_values) > 8 else ""
                    row_status = "在庫"
                    if "未售" in col_i_val: row_status = "在庫"
                    elif "收訂" in col_i_val or "客訂" in col_i_val: row_status = "已收訂"
                    elif "售" in col_i_val and "未售" not in col_i_val: row_status = "已售"
                    row_values[status_idx] = row_status
                data_to_upload_main.append(row_values)

            target_gsheet_main.clear()
            target_gsheet_main.update(values=[[str(cell) for cell in row] for row in data_to_upload_main], range_name='A1')
            if color_requests_main: doc.batch_update({"requests": color_requests_main})
        else:
            headers_main = [str(cell.value).strip() if cell.value is not None else "" for cell in ws_main[1]]
            try:
                old_values = target_gsheet_main.get_all_values()
                if old_values and len(old_values) > 1:
                    old_hdrs = [str(x).strip() for x in old_values[0]]
                    p_idx = old_hdrs.index("車牌") if "車牌" in old_hdrs else -1
                    v_idx = old_hdrs.index("車身") if "車身" in old_hdrs else -1
                    n_idx = old_hdrs.index("新編號") if "新編號" in old_hdrs else -1
                    for row in old_values[1:]:
                        key = ""
                        if n_idx != -1 and len(row) > n_idx and str(row[n_idx]).strip(): key = str(row[n_idx]).strip()
                        elif p_idx != -1 and len(row) > p_idx and str(row[p_idx]).strip(): key = str(row[p_idx]).strip()
                        elif v_idx != -1 and len(row) > v_idx and str(row[v_idx]).strip(): key = str(row[v_idx]).strip()
                        if key: old_keys.add(str(key).replace('.0', '').strip())
                else: is_excel_initial = True
            except: is_excel_initial = True

            col_model = headers_main.index("車型") if "車型" in headers_main else -1
            plate_idx = headers_main.index("車牌") if "車牌" in headers_main else -1
            vin_idx = headers_main.index("車身") if "車身" in headers_main else -1
            no_idx = headers_main.index("新編號") if "新編號" in headers_main else -1
            year_idx = headers_main.index("年份") if "年份" in headers_main else -1
            if "狀態" not in headers_main: headers_main.append("狀態")
            status_col_idx = headers_main.index("狀態")
            data_to_upload_main = [headers_main]
            new_count, new_cars_list = 0, []

            for row in ws_main.iter_rows(min_row=2):
                row_values = [cell.value if cell.value is not None else "" for cell in row]
                if not any(str(v).strip() for v in row_values): continue
                while len(row_values) <= status_col_idx: row_values.append("")
                while len(row_values) < len(headers_main): row_values.append("")
                n_val = str(row_values[no_idx]).replace('.0', '').strip() if no_idx != -1 else ""
                p_val = str(row_values[plate_idx]).replace('.0', '').strip() if plate_idx != -1 else ""
                v_val = str(row_values[vin_idx]).replace('.0', '').strip() if vin_idx != -1 else ""
                row_key = n_val if n_val else (p_val if p_val else v_val)
                if row_key and not is_excel_initial and row_key not in old_keys:
                    new_count += 1
                    y_val = str(row_values[year_idx]).replace('.0', '').strip() if year_idx != -1 else ""
                    if len(y_val) == 6 and y_val.isdigit(): y_val = f"{y_val[:4]}年{y_val[4:]}月"
                    elif len(y_val) == 4 and y_val.isdigit(): y_val = f"{y_val}年"
                    m_val = str(row_values[col_model]).strip() if col_model != -1 else ""
                    new_cars_list.append(f"{y_val} {m_val} #{p_val if p_val else '(無車牌)'}")
                    old_keys.add(row_key)
                has_color = False
                for cell in row:
                    if get_color_rgb(cell): has_color = True
                status_val = str(row_values[status_col_idx]).strip()
                if "取證" in status_val: row_values[status_col_idx] = "取證"
                elif "Anti已收訂" in status_val or "已收訂" in status_val: row_values[status_col_idx] = "Anti已收訂" if "Anti" in status_val else "已收訂"
                elif has_color or "已售" in status_val: row_values[status_col_idx] = "已售"
                else:
                    if not status_val: row_values[status_col_idx] = "在庫"
                data_to_upload_main.append(row_values)
            target_gsheet_main.clear(); target_gsheet_main.update(values=[[str(cell) for cell in row] for row in data_to_upload_main], range_name='A1')
        load_and_clean_data()
        return {"status": "success", "message": "處理成功"}
    except Exception as e: return {"status": "error", "message": f"處理失敗：{str(e)}"}
    finally:
        if wb: wb.close()
        gc.collect()

def get_valid_credentials(force_u=None, force_p=None):
    credentials_to_try = []
    if force_u and force_p: credentials_to_try.append((force_u, force_p))
    else:
        sheet_user, sheet_pwd = get_or_create_creds(); credentials_to_try.append((sheet_user, sheet_pwd))
        backup_list = get_backup_credentials_from_sheet()
        for bu, bp in backup_list:
            if (bu, bp) not in credentials_to_try: credentials_to_try.append((bu, bp))
    login_url = "https://www.jwincar.com.tw/manage/login/index.php"
    data_url = "https://www.jwincar.com.tw/manage/accounting/accounting_car_list.php?stock=all"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    for test_u, test_p in credentials_to_try:
        try:
            session = requests.Session(); session.headers.update(headers)
            session.post(login_url, data={"strID": test_u, "strPW": test_p, "Submit": "送出"})
            res = session.get(data_url + "&page=1", timeout=10)
            if BeautifulSoup(res.text, "html.parser").find("table", {"id": "carTable"}): return test_u, test_p
        except: continue
    return None, None

def core_sync_car_source(user_id: str, login_user: str, login_pwd: str):
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        login_url = "https://www.jwincar.com.tw/manage/login/index.php"
        data_url = "https://www.jwincar.com.tw/manage/accounting/accounting_car_list.php?stock=all"
        session.post(login_url, data={"strID": login_user, "strPW": login_pwd, "Submit": "送出"})
        pkey_map = {}
        try:
            contract_url = "https://www.jwincar.com.tw/manage/Contract/p14_contract_purchase_list.php"
            cp = 1
            last_c_row = ""
            while cp <= 3000:
                c_res = session.get(f"{contract_url}?page={cp}", timeout=10)
                c_soup = BeautifulSoup(c_res.text, "html.parser")
                c_table = c_soup.find("table")
                if not c_table: break
                c_rows = c_table.find_all("tr")
                if len(c_rows) <= 1: break
                curr_c_row = c_rows[1].text.strip()
                if curr_c_row == last_c_row: break
                last_c_row = curr_c_row
                c_headers = [th.text.strip() for th in c_rows[0].find_all(["th", "td"])]
                p_col, v_col = -1, -1
                for idx, h in enumerate(c_headers):
                    if any(kw in h for kw in ["車牌", "車號", "牌照"]): p_col = idx
                    if any(kw in h for kw in ["車身", "車架", "VIN"]): v_col = idx
                for row in c_rows[1:]:
                    tds = row.find_all("td")
                    if not tds: continue
                    row_pkey = ""
                    for td in tds:
                        btn = td.find("input", value=re.compile(r"鑑定|查定|表")) or td.find("input", onclick=re.compile(r"PKey"))
                        if btn and btn.has_attr("onclick"):
                            m = re.search(r'PKey=(\d+)', btn["onclick"])
                            if m: row_pkey = m.group(1); break
                    if row_pkey:
                        if p_col != -1 and p_col < len(tds):
                            txt = tds[p_col].text.strip().upper().replace("-", "")
                            if txt and txt not in ["—", "-", "NAN"]: pkey_map[txt] = row_pkey
                        if v_col != -1 and v_col < len(tds):
                            txt = tds[v_col].text.strip().upper()
                            if txt and txt not in ["—", "-", "NAN"]: pkey_map[txt] = row_pkey
                cp += 1
        except: pass

        all_cars, headers, valid_col_indices, raw_headers = [], [], [], []
        page_num, last_first_row, plate_idx_main, vin_idx_main = 1, "", -1, -1
        while page_num <= 3000:
            res = session.get(data_url + f"&page={page_num}")
            soup = BeautifulSoup(res.text, "html.parser")
            table = soup.find("table", {"id": "carTable"})
            if not table: break
            rows = table.find_all("tr")
            if len(rows) <= 1: break
            current_first_row = rows[1].text.strip()
            if current_first_row == last_first_row: break
            last_first_row = current_first_row

            # 💡 欄位過濾核心：在此處自動掃描並剔除 "操作" 欄位，防止任何位置的推擠與錯位！
            if page_num == 1:
                raw_headers = [th.text.replace("⇅", "").strip() for th in rows[0].find_all("th")]
                valid_col_indices = [i for i, h in enumerate(raw_headers) if h != "操作" and h != ""]
                headers = [raw_headers[i] for i in valid_col_indices]
                plate_idx_main = headers.index("車牌") if "車牌" in headers else -1
                vin_idx_main = headers.index("車身") if "車身" in headers else -1
                if "查定表PKey" not in headers: headers.append("查定表PKey")

            for row in rows[1:]:
                tds = row.find_all("td")
                if not tds: continue
                row_data = []
                for i in valid_col_indices:
                    if i < len(tds):
                        td = tds[i]; val = td.text.strip()
                        if val in ["—", "-"]: val = ""
                        if td.has_attr("title"): val = td["title"].strip()
                        if raw_headers[i] == "狀態":
                            if td.find("span", class_=re.compile(r"sold|已售")): val = "已售"
                            elif td.find("span", class_=re.compile(r"stock|在庫")): val = "在庫"
                            elif td.find("span", class_=re.compile(r"deposit|收訂")): val = "已收訂"
                        row_data.append(val)
                    else: row_data.append("")
                row_plate = str(row_data[plate_idx_main]).strip().upper().replace("-", "") if plate_idx_main != -1 else ""
                row_vin = str(row_data[vin_idx_main]).strip().upper() if vin_idx_main != -1 else ""
                row_data.append(pkey_map.get(row_plate) or pkey_map.get(row_vin) or "")
                all_cars.append(row_data)
            page_num += 1

        if len(all_cars) < 100: return {"status": "error", "message": "🚨 數據異常熔斷！"}
        df_crawled = pd.DataFrame(all_cars, columns=headers).fillna("")
        client = get_gspread_client(); doc = client.open_by_key(SHEET_ID); target_gsheet_main = doc.worksheet("E車源")
        target_gsheet_main.clear(); target_gsheet_main.update(values=[headers] + df_crawled.values.tolist(), range_name='A1')
        load_and_clean_data()
        return {"status": "success", "message": f"🤖 更新成功！共抓取 {len(all_cars)} 筆車源。"}
    except Exception as e: return {"status": "error", "message": f"爬蟲發生錯誤：{str(e)}"}

@app.get("/api/view_inspection", response_class=HTMLResponse)
def view_inspection(PKey: str = ""):
    global view_api_session
    if not PKey: return "<h1>❌ 錯誤：缺少 PKey</h1>"
    login_url = "https://www.jwincar.com.tw/manage/login/index.php"
    target_url = f"https://www.jwincar.com.tw/manage/accounting/accounting_car_inspection_view.php?PKey={PKey}"
    if view_api_session is None:
        view_api_session = requests.Session()
        view_api_session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        u, p = get_valid_credentials()
        if u and p: view_api_session.post(login_url, data={"strID": u, "strPW": p, "Submit": "送出"})
    try:
        res = view_api_session.get(target_url, timeout=10)
        html_content = res.text
        if "請輸入密碼" in html_content or "login" in res.url.lower():
            u, p = get_valid_credentials()
            view_api_session = requests.Session()
            view_api_session.post(login_url, data={"strID": u, "strPW": p, "Submit": "送出"})
            res = view_api_session.get(target_url, timeout=10); html_content = res.text
        soup = BeautifulSoup(html_content, "html.parser")
        for script in soup.find_all("script"): script.decompose()
        for meta in soup.find_all("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)}): meta.decompose()
        base_tag = soup.new_tag('base', href="https://www.jwincar.com.tw/manage/accounting/")
        if soup.head: soup.head.insert(0, base_tag)
        else: soup.insert(0, base_tag)
        style_tag = soup.new_tag('style'); style_tag.string = "body { background-color: #f3f4f6; } .print-btn { display: none !important; }"
        if soup.head: soup.head.append(style_tag)
        return str(soup)
    except Exception as e: return f"<h1>❌ 抓取失敗：{str(e)}</h1>"

@app.get("/api/sync_car_source")
def api_sync_car_source(user_id: str = "", u: str = "", p: str = ""):
    if not check_permission(user_id, "更新車源"): return {"status": "error", "message": "⛔ 權限不足！"}
    valid_u, valid_p = get_valid_credentials(u, p)
    if not valid_u: return {"status": "need_login", "message": "⚠️ 請重新輸入最新的帳號與密碼。"}
    return core_sync_car_source(user_id, valid_u, valid_p)

# ================= 其餘必備功能端點 =================
@app.post("/api/parse_ad")
async def parse_ad(request: Request):
    data = await request.json(); raw_text = data.get("text", "").strip()
    found_brand, found_model = "", ""
    lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
    for brand in KNOWN_MAKES:
        if brand.lower() in raw_text.lower(): found_brand = brand; break
    if lines:
        clean_line = re.sub(r'【.*?】', '', lines[0])
        clean_line = re.sub(r'\d{4}', '', clean_line)
        if found_brand: clean_line = re.compile(re.escape(found_brand), re.IGNORECASE).sub("", clean_line)
        found_model = clean_line.strip()
    man_date_str = ""
    m = re.search(r'(20\d{2})\s*年\s*(\d{1,2})', raw_text)
    if m: man_date_str = f"{m.group(1)}年{int(m.group(2))}月"
    mileage_str = ""
    m_m = re.search(r'里程[：:]?\s*([0-9,]+)', raw_text)
    if m_m: mileage_str = f"{m_m.group(1)}公里"
    return {"status": "success", "data": {"brand": found_brand, "model": found_model, "man_date": man_date_str, "lic_date": "", "mileage": mileage_str, "new_price": "", "store_price": "", "promo_price": "", "loan_term": "", "loan_monthly": ""}}

@app.post("/api/export_board")
async def export_board(request: Request):
    data = await request.json()
    wb = openpyxl.Workbook(); ws = wb.active; ws.title = "認證表格"
    ws.cell(row=2, column=2, value=data.get("brand", "")); ws.cell(row=3, column=2, value=data.get("model", ""))
    stream = io.BytesIO(); wb.save(stream); stream.seek(0)
    return StreamingResponse(stream, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=board.xlsx"})

@app.get("/api/my_permissions")
def get_my_permissions(user_id: str = "", user_name: str = ""):
    return {"status": "success", "permissions": {"最高管理員": "V", "E車源看板": "V", "更新車源": "V", "上傳檔案": "V"}, "is_new": False}

@app.get("/api/check_auth")
def check_auth(user_id: str = "", action: str = ""): return {"authorized": True}

@app.get("/api/refresh")
def refresh_data(): load_and_clean_data(); return {"message": "資料已更新"}

@app.get("/api/cars")
def get_cars(brand: str = "全部", location: str = "全部", prop: str = "全部", model: str = "", plate: str = "", person: str = "", min_price: float = 0.0, max_price: float = 99999.0, sort_by: str = "預設", limit: int = 100, hide_no_price: str = "false", hide_sold: str = "false", hide_cert: str = "false", hide_reserved: str = "false"):
    if cached_df is None: load_and_clean_data()
    return {"total": len(cached_df), "data": cached_df.head(limit).to_dict(orient="records")}

@app.get("/api/search_plate")
def search_plate(plate: str):
    if cached_df is None: load_and_clean_data()
    return {"status": "success", "data": cached_df.iloc[0].to_dict()}

@app.post("/api/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    filename = file.filename; contents = await file.read()
    return process_excel_file(filename, contents)

@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    try: handler.handle(body.decode("utf-8"), signature)
    except InvalidSignatureError: raise HTTPException(status_code=400)
    return "OK"

@app.get("/")
def serve_home(): return FileResponse("index.html")
@app.get("/ping")
def ping(): return {"status": "ok"}