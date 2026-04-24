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
    
    client = get_gspread_client()
    doc = client.open_by_key(SHEET_ID)
    
    dfs = []
    
    try:
        ws_main = doc.worksheet("E車源")
        df_main = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={ws_main.id}")
        df_main['is_sold_sheet'] = False
        dfs.append(df_main)
    except: pass

    if not dfs:
        df = pd.read_csv(CSV_URL)
        df['is_sold_sheet'] = False
    else:
        df = pd.concat(dfs, ignore_index=True)

    df.columns = [str(c).strip() for c in df.columns]
    
    if '採購' not in df.columns: 
        if '採購人' in df.columns: df['採購'] = df['採購人']
        elif '車輛負責人' in df.columns: df['採購'] = df['車輛負責人']
        elif '負責人' in df.columns: df['採購'] = df['負責人']
        else: df['採購'] = ""
        
    drop_cols = ['負責人', '車輛負責人', '採購人']
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    def merge_ids(r):
        n = r.get('新編號', '')
        o = r.get('舊編號', '')
        n_str = str(n).replace('.0', '').strip() if pd.notna(n) else ""
        o_str = str(o).replace('.0', '').strip() if pd.notna(o) else ""
        if n_str and n_str.lower() != 'nan' and o_str and o_str.lower() != 'nan': 
            return f"{o_str} ({n_str})" 
        if o_str and o_str.lower() != 'nan': return o_str
        if n_str and n_str.lower() != 'nan': return n_str
        return ""
    df['編號'] = df.apply(merge_ids, axis=1)

    if '網路' in df.columns:
        df['顯示價格'] = df['網路'].apply(clean_money)
        df['calc_net'] = df['網路'].apply(clean_money)
    elif '底價' in df.columns:
        df['顯示價格'] = df['底價'].apply(clean_money)
        df['calc_net'] = 0.0
    else:
        df['顯示價格'] = 0.0
        df['calc_net'] = 0.0

    if '起算' in df.columns: df['calc_start'] = df['起算'].apply(clean_money)
    else: df['calc_start'] = 0.0

    if '廠牌' in df.columns:
        def clean_brand(b):
            b = str(b).strip().upper()
            b = re.split(r'[/／]', b)[0]
            b = re.sub(r'[\u4e00-\u9fa5]', '', b).strip()
            return b
        df['廠牌'] = df['廠牌'].apply(clean_brand)

    if '年份' in df.columns:
        df['年份'] = df['年份'].astype(str)

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
            if '台北' in loc: return '北投店'
            if '桃園' in loc: return '桃園店'
            if '台中' in loc: return '台中店'
            if '高雄' in loc: return '高雄新廠'
            return loc
        df['車輛位置'] = df['車輛位置'].apply(clean_loc)

    def normalize_property(row):
        p = str(row.get('產權', '')).strip()
        c = str(row.get('公司', '')).strip()
        full_str = p + c
        if full_str == "" or full_str.lower() == "nan": return "其他"
        if '禾迪' in full_str: return '禾迪'
        if '展帆' in full_str: return '展帆'
        if '杰租' in full_str or '租車' in full_str: return '杰租'
        if '杰' in full_str: return '杰運'
        return p if p else (c if c else "其他")
    
    df['filter_property'] = df.apply(normalize_property, axis=1)
    
    if '狀態' in df.columns:
        df['is_sold'] = df.apply(lambda r: True if '已售' in str(r.get('狀態', '')) or r.get('is_sold_sheet') else False, axis=1)
        df['is_cert'] = df['狀態'].apply(lambda x: True if '取證' in str(x) else False)
    else:
        df['is_sold'] = df.get('is_sold_sheet', False)
        df['is_cert'] = False
        
    df['is_reserved'] = df.apply(lambda r: True if '已收訂' in str(r.get('狀態', '')) or '已收訂' in str(r.get('收訂狀態', '')) else False, axis=1)
    
    if '入庫日期' in df.columns:
        df['入庫_dt'] = df['入庫日期'].apply(parse_roc_date)
        
    df = df.fillna("")
    cached_df = df
    
    gc.collect() 
    return df

# ================= 🚀 API 區塊 =================
@app.get("/api/refresh")
def refresh_data():
    load_and_clean_data()
    return {"message": "資料已更新", "total_records": len(cached_df)}

@app.get("/api/options")
def get_options():
    if cached_df is None: load_and_clean_data()
    brands = sorted([str(x) for x in cached_df['廠牌'].unique() if x])
    locations = sorted([str(x) for x in cached_df['車輛位置'].unique() if x])
    props = sorted([str(x) for x in cached_df['filter_property'].unique() if x and x != "其他"])
    if "其他" in cached_df['filter_property'].unique(): props.append("其他")
    return {
        "brands": ["全部"] + brands,
        "locations": ["全部"] + locations,
        "properties": ["全部"] + props
    }

@app.get("/api/cars")
def get_cars(
    brand: str = "全部", location: str = "全部", prop: str = "全部",
    model: str = "", version: str = "", vin: str = "", plate: str = "",
    person: str = "", min_price: float = 0.0, max_price: float = 99999.0,
    sort_by: str = "預設", limit: int = 100, 
    hide_no_price: str = "false", hide_sold: str = "false", hide_cert: str = "false", hide_reserved: str = "false"
):
    if cached_df is None: load_and_clean_data()
    res = cached_df.copy()

    model = model.strip()
    version = version.strip()
    vin = vin.strip()
    plate = plate.strip()
    person = person.strip()

    if brand != "全部": res = res[res['廠牌'] == brand]
    if location != "全部": res = res[res['車輛位置'] == location]
    if prop != "全部": res = res[res['filter_property'] == prop]
    
    if model and '車型' in res.columns: res = res[res['車型'].astype(str).str.lower().str.contains(model.lower(), na=False)]
    if version and '版本' in res.columns: res = res[res['版本'].astype(str).str.lower().str.contains(version.lower(), na=False)]
    if vin and '車身' in res.columns: res = res[res['車身'].astype(str).str.lower().str.contains(vin.lower(), na=False)]
    if plate and '車牌' in res.columns: res = res[res['車牌'].astype(str).str.lower().str.contains(plate.lower(), na=False)]
    
    if person:
        mask = pd.Series(False, index=res.index)
        if '採購' in res.columns: mask = mask | res['採購'].astype(str).str.lower().str.contains(person.lower(), na=False)
        res = res[mask]

    res = res[(res['顯示價格'] >= min_price) & (res['顯示價格'] <= max_price)]

    if hide_no_price.lower() == "true": res = res[res['顯示價格'] > 0]
    if hide_sold.lower() == "true" and 'is_sold' in res.columns: 
        res = res[res['is_sold'] == False]
    if hide_cert.lower() == "true" and 'is_cert' in res.columns: 
        res = res[res['is_cert'] == False]
    if hide_reserved.lower() == "true" and 'is_reserved' in res.columns:
        res = res[res['is_reserved'] == False]

    if sort_by == "價格低到高": res = res.sort_values(by='顯示價格', ascending=True)
    elif sort_by == "價格高到低": res = res.sort_values(by='顯示價格', ascending=False)
    elif sort_by == "年份舊到新":
        if '年份' in res.columns: 
            res['年份_num'] = pd.to_numeric(res['年份'], errors='coerce').fillna(999999)
            res = res.sort_values(by='年份_num', ascending=True)
            res = res.drop(columns=['年份_num'])
    elif sort_by == "最新入庫":
        if '入庫_dt' in res.columns: res = res.sort_values(by='入庫_dt', ascending=False, na_position='last')
    elif sort_by == "最舊入庫":
        if '入庫_dt' in res.columns: res = res.sort_values(by='入庫_dt', ascending=True, na_position='last')
    else: 
        if '年份' in res.columns: 
            res['年份_num'] = pd.to_numeric(res['年份'], errors='coerce').fillna(0)
            res = res.sort_values(by='年份_num', ascending=False)
            res = res.drop(columns=['年份_num'])

    res = res.head(limit)
    if '入庫_dt' in res.columns: res = res.drop(columns=['入庫_dt'])
    res = res.fillna("")
    return {"total": len(res), "data": res.to_dict(orient="records")}

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
            else:
                new_columns.append(str(c).strip())
        df_simple.columns = new_columns
        
        df_simple = df_simple.dropna(axis=1, how='all')
        df_simple = df_simple.fillna("")
        
        gc.collect() 
        return {"status": "success", "data": df_simple.to_dict(orient="records")}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"讀取失敗：{str(e)}"}

# ================= 👥 CRM 客資 API 區塊 =================
@app.get("/api/customers")
def get_customers():
    try:
        client = get_gspread_client()
        sheet = client.open_by_key(SHEET_ID).worksheet("客資紀錄")
        
        raw_values = sheet.get_all_values()
        if not raw_values or len(raw_values) < 2:
            return {"status": "success", "data": []}
            
        headers = [str(h).strip() for h in raw_values[0]]
        records = []
        for row in raw_values[1:]:
            row_data = [str(cell).strip().lstrip("'") for cell in row]
            while len(row_data) < len(headers):
                row_data.append("")
            records.append(dict(zip(headers, row_data)))
            
        return {"status": "success", "data": list(reversed(records))}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/customers")
async def add_customer(request: Request):
    try:
        data = await request.json()
        tw_time = datetime.utcnow() + timedelta(hours=8)
        date_str = tw_time.strftime("%Y/%m/%d %H:%M")
        
        phone_str = str(data.get("phone", "")).strip()
        if phone_str.startswith("0"):
            phone_str = f"'{phone_str}"
            
        row_data = [
            date_str,
            data.get("name", ""),
            phone_str,
            data.get("needs", ""),
            data.get("memo", "")
        ]
        
        client = get_gspread_client()
        sheet = client.open_by_key(SHEET_ID).worksheet("客資紀錄")
        sheet.append_row(row_data, value_input_option='USER_ENTERED')
        return {"status": "success", "message": "客資已新增"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ================= 🚀 客資 Excel 智慧解析 =================
def process_crm_excel(filename: str, contents: bytes):
    wb = None
    try:
        wb = openpyxl.load_workbook(filename=io.BytesIO(contents), data_only=True, read_only=True)
        ws = wb[wb.sheetnames[0]]
        headers = [str(cell.value).strip() if cell.value is not None else "" for cell in ws[1]]
        
        new_customers = []
        
        for row in ws.iter_rows(min_row=2, values_only=True):
            r_dict = {}
            for i in range(min(len(headers), len(row))):
                val = row[i]
                r_dict[headers[i]] = str(val).strip() if val is not None else ""
            
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
            elif "收訂" in tags: status = "已收訂"
            elif "戰敗" in tags or "放棄" in tags or "暫緩" in tags: status = "戰敗"
            elif "賞車" in tags or "看車" in tags: status = "安排賞車"
            
            sales = r_dict.get("客戶擴充欄位-銷售業務", "")
            if not sales:
                for k, v in r_dict.items():
                    if ("業務" in k or "負責" in k) and v and "@" not in v: 
                        sales = v
                        break
            
            needs = ""
            for k, v in r_dict.items():
                if ("車" in k or "需求" in k) and "車牌" not in k and "車身" not in k and v:
                    needs = v
                    break
            
            new_customers.append({
                "日期": date_val,
                "姓名": name,
                "電話": f"'{phone}", 
                "需求車款": needs,
                "負責業務": sales,
                "狀態": status,
                "備註": memo
            })
            
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
        except Exception:
            sheet = doc.add_worksheet("客資紀錄", 1000, 10)
            old_records = []
            
        merged_dict = {}
        for rec in old_records:
            p = str(rec.get("電話", "")).replace("'", "").strip()
            if p: merged_dict[p] = rec
            
        update_count = 0
        add_count = 0
        
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
        for p, rec in merged_dict.items():
            final_data.append([str(rec.get(h, "")) for h in headers_crm])
            
        sheet.clear()
        sheet.update(values=final_data, range_name="A1")
        
        return {
            "status": "success", 
            "message": f"👥 客資同步完成！\n本次新增 {add_count} 筆，更新 {update_count} 筆，\n自動過濾 {len(new_customers) - add_count - update_count} 筆完全重複資料。"
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"客資處理失敗：{str(e)}"}
    finally:
        if wb: wb.close()
        del wb
        gc.collect()

# ================= 📄 PDF 解析核心 =================
def process_pdf_file(filename: str, contents: bytes):
    try:
        import pdfplumber
    except ImportError:
        return {"status": "error", "message": "伺服器缺少 pdfplumber 套件，請至 GitHub 於 requirements.txt 新增 'pdfplumber'。"}

    try:
        target_tab_name = "E車源"
        if "新竹" in filename:
            target_tab_name = "新竹車源"

        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        try: 
            target_gsheet = doc.worksheet(target_tab_name)
        except gspread.exceptions.WorksheetNotFound: 
            return {"status": "error", "message": f"找不到 Google Sheet 分頁「{target_tab_name}」"}

        all_rows = []
        headers = []

        with pdfplumber.open(io.BytesIO(contents)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    for row in table:
                        cleaned_row = [str(cell).replace('\n', ' ').strip() if cell is not None else "" for cell in row]
                        if not any(cleaned_row): continue
                        
                        if not headers and any(kw in str(cleaned_row) for kw in ["車牌", "廠牌", "年份", "新編號"]):
                            headers = cleaned_row
                            continue
                        
                        if headers:
                            all_rows.append(cleaned_row)

        if not headers:
            return {"status": "error", "message": "無法從 PDF 解析出表格，請確認此 PDF 是否包含明顯格線，或是由系統直接匯出。"}

        if "狀態" not in headers:
            headers.append("狀態")
        status_col_idx = headers.index("狀態")

        data_to_upload = [headers]
        for row in all_rows:
            while len(row) <= status_col_idx:
                row.append("")
            row[status_col_idx] = "在庫" 
            data_to_upload.append(row)

        color_requests = [{
            "repeatCell": {
                "range": { "sheetId": target_gsheet.id, "startRowIndex": 1 },
                "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": { "red": 1.0, "green": 1.0, "blue": 1.0 }}}},
                "fields": "userEnteredFormat.backgroundColorStyle,userEnteredFormat.backgroundColor"
            }
        }]

        target_gsheet.clear()
        stringified_main = [[str(cell) if cell is not None else "" for cell in row] for row in data_to_upload]
        target_gsheet.update(values=stringified_main, range_name='A1')
        doc.batch_update({"requests": color_requests})
        
        load_and_clean_data()
        
        return {"status": "success", "message": f"📄 PDF 解析成功！\n共更新 {len(data_to_upload)-1} 筆車輛，已自動全數標記為「在庫」！"}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"PDF 處理失敗：{str(e)}"}
    finally:
        gc.collect()

# ================= Excel 解析與上傳 =================
def get_color_rgb(cell):
    try:
        fill = cell.fill
        if not fill: return None
        color = getattr(fill, 'fgColor', None) or getattr(fill, 'start_color', None)
        if not color: return None
        
        rgb_hex = None
        if hasattr(color, 'rgb') and color.rgb and isinstance(color.rgb, str):
            rgb_hex = color.rgb
        elif hasattr(color, 'type') and color.type == 'indexed':
            from openpyxl.styles.colors import COLOR_INDEX
            idx = color.indexed
            if isinstance(idx, int) and idx < len(COLOR_INDEX): rgb_hex = COLOR_INDEX[idx]
        elif hasattr(color, 'type') and color.type == 'theme':
            theme_colors = ["FFFFFF", "000000", "E7E6E6", "44546A", "4472C4", "ED7D31", "A5A5A5", "FFC000", "5B9BD5", "70AD47"]
            idx = color.theme
            if isinstance(idx, int) and idx < len(theme_colors): rgb_hex = theme_colors[idx]
                
        if rgb_hex and isinstance(rgb_hex, str):
            rgb_hex = rgb_hex.replace('#', '')
            if rgb_hex in ['00000000', 'FFFFFFFF']: return None
            if len(rgb_hex) == 8: rgb_hex = rgb_hex[2:] 
            if len(rgb_hex) == 6:
                return (int(rgb_hex[0:2], 16) / 255.0, int(rgb_hex[2:4], 16) / 255.0, int(rgb_hex[4:6], 16) / 255.0)
    except: pass
    return None

def process_excel_file(filename: str, contents: bytes):
    wb = None
    try:
        target_tab_name = "E車源"
        if "新竹" in filename:
            target_tab_name = "新竹車源"
            
        wb = openpyxl.load_workbook(filename=io.BytesIO(contents), data_only=True)
        ws_main = wb[wb.sheetnames[0]]
        headers_main = [str(cell.value).strip() if cell.value is not None else "" for cell in ws_main[1]]
        
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        
        try: target_gsheet_main = doc.worksheet(target_tab_name)
        except gspread.exceptions.WorksheetNotFound: return {"status": "error", "message": f"找不到分頁「{target_tab_name}」"}

        data_to_upload_main = []
        color_requests_main = [{
            "repeatCell": {
                "range": { "sheetId": target_gsheet_main.id, "startRowIndex": 1 },
                "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": { "red": 1.0, "green": 1.0, "blue": 1.0 }}}},
                "fields": "userEnteredFormat.backgroundColorStyle,userEnteredFormat.backgroundColor"
            }
        }]

        # 🚀 尋找舊的車輛資料，準備比對新增了幾台車
        old_keys = set()
        try:
            old_values = target_gsheet_main.get_all_values()
            if old_values and len(old_values) > 1:
                old_hdrs = [str(x).strip() for x in old_values[0]]
                p_idx = old_hdrs.index("車牌") if "車牌" in old_hdrs else -1
                v_idx = old_hdrs.index("車身") if "車身" in old_hdrs else -1
                n_idx = old_hdrs.index("新編號") if "新編號" in old_hdrs else -1
                m_idx = old_hdrs.index("車型") if "車型" in old_hdrs else -1
                
                for row in old_values[1:]:
                    key = ""
                    if n_idx != -1 and len(row) > n_idx and str(row[n_idx]).strip(): key = str(row[n_idx]).strip()
                    elif p_idx != -1 and len(row) > p_idx and str(row[p_idx]).strip(): key = str(row[p_idx]).strip()
                    elif v_idx != -1 and len(row) > v_idx and str(row[v_idx]).strip(): key = str(row[v_idx]).strip()
                    
                    if not key and m_idx != -1 and len(row) > m_idx: key = "M_" + str(row[m_idx]).strip()
                    if key: old_keys.add(key)
        except Exception:
            pass

        col_model = headers_main.index("車型") if "車型" in headers_main else -1
        col_version = headers_main.index("版本") if "版本" in headers_main else -1
        plate_idx = headers_main.index("車牌") if "車牌" in headers_main else -1
        vin_idx = headers_main.index("車身") if "車身" in headers_main else -1
        no_idx = headers_main.index("新編號") if "新編號" in headers_main else -1
        year_idx = headers_main.index("年份") if "年份" in headers_main else -1
        
        new_cars_list = []

        if target_tab_name == "新竹車源":
            if "收訂狀態" not in headers_main: headers_main.append("收訂狀態")
            status_idx = headers_main.index("收訂狀態")
            data_to_upload_main = [headers_main]
            
            for row in ws_main.iter_rows(min_row=2):
                row_values = [cell.value if cell.value is not None else "" for cell in row]
                if not any(str(v).strip() for v in row_values): continue
                
                while len(row_values) <= status_idx: row_values.append("")
                while len(row_values) < len(headers_main): row_values.append("")
                
                # 🚀 判斷是否為新車，並格式化為 YYYY年MM月
                p_val = str(row_values[plate_idx]).strip() if plate_idx != -1 else ""
                v_val = str(row_values[vin_idx]).strip() if vin_idx != -1 else ""
                n_val = str(row_values[no_idx]).strip() if no_idx != -1 else ""
                m_val = str(row_values[col_model]).strip() if col_model != -1 else ""
                
                row_key = n_val if n_val else (p_val if p_val else (v_val if v_val else ("M_" + m_val if m_val else "")))
                
                if row_key and row_key not in old_keys:
                    y_val = str(row_values[year_idx]).strip() if year_idx != -1 else ""
                    if len(y_val) == 6 and y_val.replace(".0", "").isdigit(): y_val = f"{y_val[:4]}年{y_val[4:]}月"
                    elif len(y_val) == 4 and y_val.isdigit(): y_val = f"{y_val}年"
                    disp_plate = p_val if p_val else "(無車牌)"
                    new_cars_list.append(f"{y_val} {m_val} #{disp_plate}")
                    old_keys.add(row_key)
                
                is_reserved = False
                target_row_idx = len(data_to_upload_main)
                for c_idx, cell in enumerate(row):
                    rgb = get_color_rgb(cell)
                    if rgb:
                        color_requests_main.append({
                            "repeatCell": {
                                "range": { "sheetId": target_gsheet_main.id, "startRowIndex": target_row_idx, "endRowIndex": target_row_idx + 1, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1 },
                                "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": { "red": rgb[0], "green": rgb[1], "blue": rgb[2] }}}},
                                "fields": "userEnteredFormat.backgroundColorStyle"
                            }
                        })
                        if c_idx == col_model or c_idx == col_version: is_reserved = True
                            
                row_values[status_idx] = "已收訂" if is_reserved else ""
                data_to_upload_main.append(row_values)
                
            messages = []
            try:
                target_gsheet_main.clear()
                stringified_main = [[str(cell) if cell is not None else "" for cell in row] for row in data_to_upload_main]
                target_gsheet_main.update(values=stringified_main, range_name='A1')
                target_gsheet_main.update_acell('A2', '="共"&SUMPRODUCT(--(LEN(TRIM($C$5:$C$133))>0))&"台"')
                doc.batch_update({"requests": color_requests_main})
                
                msg = f"「新竹車源」更新成功({len(data_to_upload_main)-1}筆)"
                if new_cars_list:
                    msg += f"\n✨ 新增 {len(new_cars_list)} 台車輛：\n" + "\n".join(new_cars_list[:10])
                    if len(new_cars_list) > 10: msg += f"\n...等共 {len(new_cars_list)} 台"
                else:
                    msg += "\n🔄 資料已同步，本次無新增車輛。"
                messages.append(msg)
                
            except Exception as e: return {"status": "error", "message": f"新竹寫入失敗：{str(e)}"}

        else:
            if "狀態" not in headers_main: headers_main.append("狀態")
            status_col_idx = headers_main.index("狀態")
            data_to_upload_main = [headers_main]

            for row in ws_main.iter_rows(min_row=2):
                row_values = [cell.value if cell.value is not None else "" for cell in row]
                if not any(str(v).strip() for v in row_values): continue
                
                while len(row_values) <= status_col_idx: row_values.append("")
                while len(row_values) < len(headers_main): row_values.append("")
                
                # 🚀 判斷是否為新車，並格式化為 YYYY年MM月
                p_val = str(row_values[plate_idx]).strip() if plate_idx != -1 else ""
                v_val = str(row_values[vin_idx]).strip() if vin_idx != -1 else ""
                n_val = str(row_values[no_idx]).strip() if no_idx != -1 else ""
                m_val = str(row_values[col_model]).strip() if col_model != -1 else ""
                
                row_key = n_val if n_val else (p_val if p_val else (v_val if v_val else ("M_" + m_val if m_val else "")))
                
                if row_key and row_key not in old_keys:
                    y_val = str(row_values[year_idx]).strip() if year_idx != -1 else ""
                    if len(y_val) == 6 and y_val.replace(".0", "").isdigit(): y_val = f"{y_val[:4]}年{y_val[4:]}月"
                    elif len(y_val) == 4 and y_val.isdigit(): y_val = f"{y_val}年"
                    disp_plate = p_val if p_val else "(無車牌)"
                    new_cars_list.append(f"{y_val} {m_val} #{disp_plate}")
                    old_keys.add(row_key)
                
                has_color = False
                row_colors = []
                for cell in row:
                    rgb = get_color_rgb(cell)
                    row_colors.append(rgb)
                    if rgb: has_color = True

                status_val = str(row_values[status_col_idx]).strip()
                if "取證" in status_val: row_values[status_col_idx] = "取證"
                elif "已收訂" in status_val: row_values[status_col_idx] = "已收訂"
                elif has_color or "已售" in status_val: row_values[status_col_idx] = "已售"
                else:
                    if not status_val: row_values[status_col_idx] = "在庫"
                        
                target_row_idx = len(data_to_upload_main) 
                data_to_upload_main.append(row_values)
                
                for c_idx, rgb in enumerate(row_colors):
                    if rgb:
                        color_requests_main.append({
                            "repeatCell": {
                                "range": { "sheetId": target_gsheet_main.id, "startRowIndex": target_row_idx, "endRowIndex": target_row_idx + 1, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1 },
                                "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": { "red": rgb[0], "green": rgb[1], "blue": rgb[2] }}}},
                                "fields": "userEnteredFormat.backgroundColorStyle"
                            }
                        })

            messages = []
            try:
                target_gsheet_main.clear()
                stringified_main = [[str(cell) if cell is not None else "" for cell in row] for row in data_to_upload_main]
                target_gsheet_main.update(values=stringified_main, range_name='A1')
                doc.batch_update({"requests": color_requests_main})
                
                msg = f"「E車源」成功({len(data_to_upload_main)-1}筆)"
                if new_cars_list:
                    msg += f"\n✨ 新增 {len(new_cars_list)} 台車輛：\n" + "\n".join(new_cars_list[:10])
                    if len(new_cars_list) > 10: msg += f"\n...等共 {len(new_cars_list)} 台"
                else:
                    msg += "\n🔄 資料已同步，本次無新增車輛。"
                messages.append(msg)
                
                # 自動備份售出清單
                try: sold_gsheet = doc.worksheet("E車源售出")
                except gspread.exceptions.WorksheetNotFound: sold_gsheet = doc.add_worksheet(title="E車源售出", rows="1000", cols="30")
                try: old_records = sold_gsheet.get_all_records()
                except Exception: old_records = []
                
                new_records = []
                for row in data_to_upload_main[1:]:
                    st = str(row[status_col_idx]).strip()
                    if st and st != "在庫":
                        padded = list(row)
                        while len(padded) < len(headers_main): padded.append("")
                        new_records.append(dict(zip(headers_main, padded)))
                        
                if new_records or old_records:
                    merged_dict = {}
                    for rec in old_records:
                        pk = str(rec.get("車牌", "")).strip() or str(rec.get("新編號", "")).strip() or str(rec.get("車身", "")).strip()
                        if pk: merged_dict[pk] = rec
                    for rec in new_records:
                        pk = str(rec.get("車牌", "")).strip() or str(rec.get("新編號", "")).strip() or str(rec.get("車身", "")).strip()
                        if pk: merged_dict[pk] = rec
                        else: merged_dict[str(uuid.uuid4())] = rec
                        
                    final_headers = list(headers_main)
                    for rec in merged_dict.values():
                        for k in rec.keys():
                            if k not in final_headers and str(k).strip(): final_headers.append(k)
                            
                    final_data = [final_headers]
                    for rec in merged_dict.values(): final_data.append([str(rec.get(h, "")) for h in final_headers])
                        
                    sold_gsheet.clear()
                    sold_gsheet.update(values=final_data, range_name='A1')
                    messages.append("並已同步備份至「E車源售出」")
                
            except Exception as e: return {"status": "error", "message": f"主表寫入失敗：{str(e)}"}

        load_and_clean_data()
        return {"status": "success", "message": " ＆ ".join(messages)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"處理失敗：{str(e)}"}
    finally:
        if wb: wb.close()
        del wb
        gc.collect()

# ================= 🚀 終極外掛：自動化爬取公司後台車源表 =================
@app.get("/api/sync_car_source")
def sync_car_source_from_backend():
    global cached_df
    try:
        login_url = "https://www.jwincar.com.tw/manage/login/index.php"
        data_url = "https://www.jwincar.com.tw/manage/accounting/accounting_car_list.php?stock=all"
        
        session = requests.Session()
        login_payload = {
            "strID": "Admin02",
            "strPW": "Eric740625",
            "Submit": "送出"
        }
        
        login_res = session.post(login_url, data=login_payload)
        
        res = session.get(data_url + "&page=1")
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, "html.parser")
        
        table = soup.find("table", {"id": "carTable"})
        if not table:
            return {"status": "error", "message": "🚨 系統警告：找不到後台的車源表格！可能網頁格式已更改或登入失效。為保護資料，已停止更新，請暫時改用「上傳 Excel 或 PDF」的方式更新車源。"}
        
        total_pages = 1
        page_info = soup.find(string=re.compile(r"第 \d+ / \d+ 頁"))
        if page_info:
            match = re.search(r"/ (\d+) 頁", page_info)
            if match:
                total_pages = int(match.group(1))

        all_cars = []
        headers = []

        for p in range(1, total_pages + 1):
            if p > 1:
                res = session.get(data_url + f"&page={p}")
                res.encoding = 'utf-8'
                soup = BeautifulSoup(res.text, "html.parser")
                
            table = soup.find("table", {"id": "carTable"})
            if not table: continue
            rows = table.find_all("tr")
            if not rows: continue
            
            if p == 1:
                th_elements = rows[0].find_all("th")
                headers = [th.text.replace("⇅", "").strip() for th in th_elements]
                if "狀態" not in headers or "新編號" not in headers or "廠牌" not in headers:
                    return {"status": "error", "message": "🚨 系統警告：後台表格的「核心欄位(狀態/新編號/廠牌)」遺失！網頁結構可能已大改，為保護資料，已停止更新。"}
                
            for row in rows[1:]:
                tds = row.find_all("td")
                if not tds: continue
                
                row_data = []
                for idx, td in enumerate(tds):
                    val = td.text.strip()
                    if val == "—" or val == "-": val = ""
                    if td.has_attr("title"): val = td["title"].strip()
                    
                    if headers[idx] == "狀態":
                        if td.find("span", class_="sold-badge") or td.find("span", string="已售"): val = "已售"
                        elif td.find("span", class_="in-stock-badge"): val = "在庫"
                        elif td.find("span", class_="deposit-badge"): val = "已收訂"
                        
                    row_data.append(val)
                all_cars.append(row_data)

        if len(all_cars) < 50:
            return {"status": "error", "message": f"🚨 系統警告：抓取到的車輛數量異常少 (僅 {len(all_cars)} 台)！網頁可能載入不完全，為避免清空現有資料，已停止更新。"}

        df_crawled = pd.DataFrame(all_cars, columns=headers)
        if "操作" in df_crawled.columns: df_crawled = df_crawled.drop(columns=["操作"])
        df_crawled = df_crawled.fillna("")

        # 🚀 尋找舊的編號，準備比對新車數量
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
                        
                    m = str(row.get("車型", "")).strip()
                    p_val = str(row.get("車牌", "")).strip()
                    new_cars_list.append(f"{y} {m} #{p_val}")
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
        
        msg = f"🤖 爬蟲任務完成！共成功抓取 {len(all_cars)} 筆車源。"
        if new_count > 0:
            msg += f"\n✨ 這次自動發現了 {new_count} 台新車！"
            if new_cars_list:
                msg += "\n" + "\n".join(new_cars_list[:10])
                if len(new_cars_list) > 10: msg += f"\n...等共 {new_count} 台"
        else:
            msg += "\n🔄 資料已同步，本次無新增車輛。"
            
        return {"status": "success", "message": msg}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"爬蟲執行失敗：{str(e)}"}
    finally:
        gc.collect()

@app.post("/api/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    filename = file.filename
    contents = await file.read()
    if filename.lower().endswith('.pdf'):
        return process_pdf_file(filename, contents)
    elif "customer" in filename.lower() or "客資" in filename:
        return process_crm_excel(filename, contents)
    else:
        return process_excel_file(filename, contents)

@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    body_str = body.decode("utf-8")
    try: handler.handle(body_str, signature)
    except InvalidSignatureError: raise HTTPException(status_code=400, detail="Invalid signature")
    return "OK"

@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = event.message.text.strip()
    
    if text == "更新車源" or text == "抓取車源":
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="🤖 收到指令！正在連線公司後台爬取最新車源資料，這大約需要 30 秒，請稍候..."))
        def run_crawler():
            try:
                res = sync_car_source_from_backend()
                if res["status"] == "success": line_bot_api.push_message(event.source.user_id, TextSendMessage(text=res["message"]))
                else: line_bot_api.push_message(event.source.user_id, TextSendMessage(text="❌ " + res["message"]))
            except Exception as e: line_bot_api.push_message(event.source.user_id, TextSendMessage(text=f"❌ 發生系統錯誤：\n{str(e)}"))
        threading.Thread(target=run_crawler).start()
        return
    
    if text.startswith("客資") or text.startswith("記客資"):
        try:
            parts = [p.strip() for p in text.split('/')]
            if len(parts) >= 4:
                name = parts[1]
                phone = parts[2].strip()
                needs = parts[3]
                memo = parts[4] if len(parts) > 4 else ""
                
                phone_val = f"'{phone}" if phone.startswith("0") else phone
                tw_time = datetime.utcnow() + timedelta(hours=8)
                date_str = tw_time.strftime("%Y/%m/%d %H:%M")
                
                client = get_gspread_client()
                sheet = client.open_by_key(SHEET_ID).worksheet("客資紀錄")
                sheet.append_row([date_str, name, phone_val, needs, memo], value_input_option='USER_ENTERED')
                reply = f"✅ 客資建檔成功！\n姓名：{name}\n電話：{phone}\n需求：{needs}"
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
            else: line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 格式錯誤！請輸入：\n客資 / 姓名 / 電話 / 需求 / 備註"))
        except Exception as e: line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 寫入失敗：{str(e)}"))
        return

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="🤖 您好！我是自動小幫手。\n\n▶️ 【車源更新】請對我說：「更新車源」\n▶️ 【客資上傳】請直接傳送 Excel 檔案\n▶️ 【手動記客】客資 / 姓名 / 電話 / 找什麼車 / 備註"))

@handler.add(MessageEvent, message=FileMessage)
def handle_file_message(event):
    message_id = event.message.id
    filename = event.message.file_name
    
    is_excel = filename.lower().endswith('.xlsx')
    is_pdf = filename.lower().endswith('.pdf')
    
    if not (is_excel or is_pdf):
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 老闆，請上傳 .xlsx 或是 .pdf 格式的檔案喔！"))
        return
    
    reply_msg = "⏳ 收到檔案！正在幫您解析資料與同步雲端，請稍候...\n(處理完成後會自動回報)"
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_msg))
    
    def process_and_notify():
        try:
            message_content = line_bot_api.get_message_content(message_id)
            contents = b"".join([chunk for chunk in message_content.iter_content()])
            
            if is_pdf: result = process_pdf_file(filename, contents)
            elif "customer" in filename.lower() or "客資" in filename: result = process_crm_excel(filename, contents)
            else: result = process_excel_file(filename, contents)
                
            if result["status"] == "success": line_bot_api.push_message(event.source.user_id, TextSendMessage(text="✅ 處理完成！\n" + result["message"]))
            else: line_bot_api.push_message(event.source.user_id, TextSendMessage(text="❌ 處理失敗：\n" + result["message"]))
        except Exception as e: line_bot_api.push_message(event.source.user_id, TextSendMessage(text=f"❌ 發生系統錯誤：\n{str(e)}"))
        finally: gc.collect()

    threading.Thread(target=process_and_notify).start()

@app.get("/")
def serve_home(): return FileResponse("index.html")
@app.head("/")
@app.get("/ping")
def ping(): return {"status": "ok"}
@app.get("/cars")
def serve_cars(): return FileResponse("cars.html")
@app.get("/deal")
def serve_deal(): return FileResponse("deal.html")
@app.get("/loan")
def serve_loan(): return FileResponse("loan.html")
@app.get("/dispatch")
def serve_dispatch(): return FileResponse("dispatch.html")
@app.get("/simple")
def serve_simple(): return FileResponse("simple.html")
@app.get("/tax")
def serve_tax(): return FileResponse("tax.html")
@app.get("/cs")
def serve_cs(): return FileResponse("cs.html")
@app.get("/copy")
def serve_copy(): return FileResponse("copy.html")
@app.get("/customer")
def serve_customer(): return FileResponse("customer.html")
