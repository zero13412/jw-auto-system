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
from datetime import datetime, timedelta

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
        
    if '收訂狀態' in df.columns:
        df['is_reserved'] = df['收訂狀態'].apply(lambda x: True if str(x).strip() == "已收訂" else False)
    else:
        df['is_reserved'] = False 
    
    if '入庫日期' in df.columns:
        df['入庫_dt'] = df['入庫日期'].apply(parse_roc_date)
        
    df = df.fillna("")
    cached_df = df
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
    hide_no_price: str = "false", hide_sold: str = "false", hide_cert: str = "false"
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

        # 開啟 PDF 並提取表格
        with pdfplumber.open(io.BytesIO(contents)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    for row in table:
                        cleaned_row = [str(cell).replace('\n', ' ').strip() if cell is not None else "" for cell in row]
                        # 略過全空行
                        if not any(cleaned_row): continue
                        
                        # 尋找標題列
                        if not headers and any(kw in str(cleaned_row) for kw in ["車牌", "廠牌", "年份", "新編號"]):
                            headers = cleaned_row
                            continue
                        
                        # 收集資料列
                        if headers:
                            all_rows.append(cleaned_row)

        if not headers:
            return {"status": "error", "message": "無法從 PDF 解析出表格，請確認此 PDF 是否包含明顯格線，或是由系統直接匯出。"}

        # 強制加入「狀態」欄位
        if "狀態" not in headers:
            headers.append("狀態")
        status_col_idx = headers.index("狀態")

        data_to_upload = [headers]
        for row in all_rows:
            # 防呆：確保列長度足夠
            while len(row) <= status_col_idx:
                row.append("")
            # 👉 核心邏輯：PDF 上傳的車輛，無條件全部標記為「在庫」
            row[status_col_idx] = "在庫" 
            data_to_upload.append(row)

        # 準備清除背景顏色（以免被舊 Excel 的底色影響）
        color_requests = [{
            "repeatCell": {
                "range": { "sheetId": target_gsheet.id, "startRowIndex": 1 },
                "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": { "red": 1.0, "green": 1.0, "blue": 1.0 }}}},
                "fields": "userEnteredFormat.backgroundColorStyle,userEnteredFormat.backgroundColor"
            }
        }]

        # 寫入 Google Sheet
        target_gsheet.clear()
        stringified_main = [[str(cell) if cell is not None else "" for cell in row] for row in data_to_upload]
        target_gsheet.update(values=stringified_main, range_name='A1')
        doc.batch_update({"requests": color_requests})
        
        # 觸發快取更新
        load_and_clean_data()
        
        return {"status": "success", "message": f"📄 PDF 解析成功！\n共更新 {len(data_to_upload)-1} 筆車輛，已自動全數標記為「在庫」！"}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"PDF 處理失敗：{str(e)}"}


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

        if target_tab_name == "新竹車源":
            # --- 🛡️ 新竹店專屬舊格式 ---
            col_model = headers_main.index("車型") if "車型" in headers_main else -1
            col_version = headers_main.index("版本") if "版本" in headers_main else -1
            
            if "收訂狀態" not in headers_main: 
                headers_main.append("收訂狀態")
            status_idx = headers_main.index("收訂狀態")
            
            data_to_upload_main = [headers_main]
            
            for row in ws_main.iter_rows(min_row=2):
                row_values = [cell.value if cell.value is not None else "" for cell in row]
                if not any(str(v).strip() for v in row_values): continue
                
                # 🚨 極致防呆：確保每一行的長度絕對足夠
                while len(row_values) <= status_idx:
                    row_values.append("")
                while len(row_values) < len(headers_main): 
                    row_values.append("")
                
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
                        if c_idx == col_model or c_idx == col_version:
                            is_reserved = True
                            
                row_values[status_idx] = "已收訂" if is_reserved else ""
                data_to_upload_main.append(row_values)
                
            messages = []
            try:
                target_gsheet_main.clear()
                stringified_main = [[str(cell) if cell is not None else "" for cell in row] for row in data_to_upload_main]
                target_gsheet_main.update(values=stringified_main, range_name='A1')
                target_gsheet_main.update_acell('A2', '="共"&SUMPRODUCT(--(LEN(TRIM($C$5:$C$133))>0))&"台"')
                doc.batch_update({"requests": color_requests_main})
                messages.append(f"「新竹車源」更新成功({len(data_to_upload_main)-1}筆)")
            except Exception as e: return {"status": "error", "message": f"新竹寫入失敗：{str(e)}"}

        else:
            # --- 🚀 會計部 E車源新格式 ---
            if "狀態" not in headers_main:
                headers_main.append("狀態")
            status_col_idx = headers_main.index("狀態")
            
            data_to_upload_main = [headers_main]

            for row in ws_main.iter_rows(min_row=2):
                row_values = [cell.value if cell.value is not None else "" for cell in row]
                if not any(str(v).strip() for v in row_values): continue
                
                # 🚨 極致防呆
                while len(row_values) <= status_col_idx:
                    row_values.append("")
                while len(row_values) < len(headers_main): 
                    row_values.append("")
                
                has_color = False
                row_colors = []
                
                for cell in row:
                    rgb = get_color_rgb(cell)
                    row_colors.append(rgb)
                    if rgb: has_color = True

                status_val = str(row_values[status_col_idx]).strip()
                
                if "取證" in status_val:
                    row_values[status_col_idx] = "取證"
                elif has_color or "已售" in status_val:
                    row_values[status_col_idx] = "已售"
                else:
                    if not status_val:
                        row_values[status_col_idx] = "在庫"
                        
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
                messages.append(f"「E車源」成功({len(data_to_upload_main)-1}筆)")
            except Exception as e: return {"status": "error", "message": f"主表寫入失敗：{str(e)}"}

        load_and_clean_data()
        return {"status": "success", "message": " ＆ ".join(messages)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"處理失敗：{str(e)}"}

@app.post("/api/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    filename = file.filename
    contents = await file.read()
    if filename.lower().endswith('.pdf'):
        return process_pdf_file(filename, contents)
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
            
            if is_pdf:
                result = process_pdf_file(filename, contents)
            else:
                result = process_excel_file(filename, contents)
                
            if result["status"] == "success": 
                line_bot_api.push_message(event.source.user_id, TextSendMessage(text="✅ 處理完成！\n" + result["message"]))
            else: 
                line_bot_api.push_message(event.source.user_id, TextSendMessage(text="❌ 處理失敗：\n" + result["message"]))
        except Exception as e: 
            line_bot_api.push_message(event.source.user_id, TextSendMessage(text=f"❌ 發生系統錯誤：\n{str(e)}"))

    threading.Thread(target=process_and_notify).start()

# ================= 🚀 LINE Bot 秒記客資功能 =================
@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = event.message.text.strip()
    
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
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 格式錯誤！請輸入：\n客資 / 姓名 / 電話 / 需求 / 備註"))
        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 寫入失敗：{str(e)}"))
        return

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text="🤖 您好！我是自動上傳小幫手。\n請直接將 Excel 或 PDF 檔案傳到這裡，我就會幫您自動同步！\n\n📝 記客資請輸入：\n客資 / 姓名 / 電話 / 找什麼車 / 備註")
    )

# ================= 網頁路由區塊 =================
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