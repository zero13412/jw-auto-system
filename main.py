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

app = FastAPI(title="🚗 杰運汽車 - 內部系統 API")

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
    df = pd.read_csv(CSV_URL)
    df.columns = [str(c).strip() for c in df.columns]
    
    if '負責人' not in df.columns:
        if '車輛負責人' in df.columns: df['負責人'] = df['車輛負責人']
        elif '採購人' in df.columns: df['負責人'] = df['採購人']
        else: df['負責人'] = ""
            
    if '採購' not in df.columns: 
        if '採購人' in df.columns: df['採購'] = df['採購人']
        else: df['採購'] = ""
            
    if '新編號' in df.columns or '舊編號' in df.columns:
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

    # ==========================================
    # 【修復】：終極版廠牌清理，強制抹除所有中文字與斜線
    # ==========================================
    if '廠牌' in df.columns:
        def clean_brand(b):
            b = str(b).strip().upper()
            # 1. 遇到半形或全形斜線，只取前面
            b = re.split(r'[/／]', b)[0]
            # 2. 橡皮擦機制：把所有中文字清空
            b = re.sub(r'[\u4e00-\u9fa5]', '', b).strip()
            return b
        df['廠牌'] = df['廠牌'].apply(clean_brand)

    if '年份' in df.columns:
        df['年份'] = df['年份'].astype(str).str[:4]

    if '里程' in df.columns:
        def clean_mileage(m):
            try:
                m_str = str(m).replace(',', '').strip()
                matches = re.findall(r"(\d+\.?\d*)", m_str)
                if matches:
                    val = float(matches[0])
                    if val > 1000: return round(val / 10000, 1)
                    return val
                return m
            except:
                return m
        df['里程'] = df['里程'].apply(clean_mileage)

    if '車輛位置' in df.columns:
        def clean_loc(loc):
            loc = str(loc).strip()
            if '台北' in loc: return '北投店'
            if '新竹' in loc: return '新竹店'
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
    hide_no_price: str = "false", hide_reserved: str = "false"
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
        if '負責人' in res.columns: mask = mask | res['負責人'].astype(str).str.lower().str.contains(person.lower(), na=False)
        if '採購' in res.columns: mask = mask | res['採購'].astype(str).str.lower().str.contains(person.lower(), na=False)
        res = res[mask]

    res = res[(res['顯示價格'] >= min_price) & (res['顯示價格'] <= max_price)]

    if hide_no_price.lower() == "true": res = res[res['顯示價格'] > 0]
    if hide_reserved.lower() == "true": res = res[res['is_reserved'] == False]

    if sort_by == "價格低到高": res = res.sort_values(by='顯示價格', ascending=True)
    elif sort_by == "價格高到低": res = res.sort_values(by='顯示價格', ascending=False)
    elif sort_by == "年份舊到新":
        if '年份' in res.columns: 
            res['年份_num'] = pd.to_numeric(res['年份'], errors='coerce').fillna(9999)
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
        target_tab_name = "新竹車源" if "新竹" in filename else "E車源"
        wb = openpyxl.load_workbook(filename=io.BytesIO(contents), data_only=True)
        
        sheet_name_main = next((name for name in wb.sheetnames if "車源證件資料" in name), wb.sheetnames[0])
        ws_main = wb[sheet_name_main]
        headers_main = [str(cell.value).strip() if cell.value is not None else "" for cell in ws_main[1]]
        col_model = headers_main.index("車型") if "車型" in headers_main else -1
        col_version = headers_main.index("版本") if "版本" in headers_main else -1
        
        if "收訂狀態" not in headers_main: headers_main.append("收訂狀態")
        status_idx = headers_main.index("收訂狀態")
        
        data_to_upload_main = [headers_main]
        
        client = get_gspread_client()
        doc = client.open_by_key(SHEET_ID)
        
        try: target_gsheet_main = doc.worksheet(target_tab_name)
        except gspread.exceptions.WorksheetNotFound: return {"status": "error", "message": f"找不到分頁「{target_tab_name}」"}

        old_plates = set()
        if target_tab_name == "新竹車源":
            try:
                old_values = target_gsheet_main.get_all_values()
                for i, r in enumerate(old_values[:10]):
                    r_str = [str(x).strip() for x in r]
                    if "車牌" in r_str or "車型" in r_str:
                        idx = r_str.index("車牌") if "車牌" in r_str else -1
                        if idx != -1:
                            for row_val in old_values[i+1:]:
                                if len(row_val) > idx:
                                    p = str(row_val[idx]).strip().upper()
                                    if p: old_plates.add(p)
                        break
            except: pass 

        color_requests_main = [{
            "repeatCell": {
                "range": { "sheetId": target_gsheet_main.id, "startRowIndex": 1 },
                "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": { "red": 1.0, "green": 1.0, "blue": 1.0 }}}},
                "fields": "userEnteredFormat.backgroundColorStyle,userEnteredFormat.backgroundColor"
            }
        }]
        
        for row in ws_main.iter_rows(min_row=2):
            row_values = [cell.value if cell.value is not None else "" for cell in row]
            if not any(str(v).strip() for v in row_values): continue
            while len(row_values) < len(headers_main): row_values.append("")
            
            target_row_idx = len(data_to_upload_main) 
            is_reserved = False
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
                if c_idx == col_model and rgb: is_reserved = True
                if not is_reserved and c_idx == col_version and rgb: is_reserved = True
                    
            row_values[status_idx] = "已收訂" if is_reserved else ""
            data_to_upload_main.append(row_values)

        new_cars_msg_list = []
        if target_tab_name == "新竹車源" and old_plates:
            for i, r in enumerate(data_to_upload_main[:10]):
                r_str = [str(x).strip() for x in r]
                if "車牌" in r_str or "車型" in r_str:
                    idx_y = r_str.index("年份") if "年份" in r_str else -1
                    idx_m = r_str.index("車型") if "車型" in r_str else -1
                    idx_c = r_str.index("顏色") if "顏色" in r_str else -1
                    idx_p = r_str.index("車牌") if "車牌" in r_str else -1
                    for row_vals in data_to_upload_main[i+1:]:
                        plate = str(row_vals[idx_p]).strip().upper() if idx_p != -1 and len(row_vals) > idx_p else ""
                        if plate and plate not in old_plates:
                            year = str(row_vals[idx_y]) if idx_y != -1 and len(row_vals) > idx_y else ""
                            model = str(row_vals[idx_m]) if idx_m != -1 and len(row_vals) > idx_m else ""
                            color = str(row_vals[idx_c]) if idx_c != -1 and len(row_vals) > idx_c else ""
                            if model and str(model).strip().lower() != "nan":
                                year = re.sub(r'\.0$', '', year)
                                new_cars_msg_list.append(f"🔸 {year} {model} {color}  #{plate or '無牌'}")
                    break

        data_to_upload_sold = []
        sheet_name_sold = next((name for name in wb.sheetnames if "已售" in name), None)
        color_requests_sold = []
        target_gsheet_sold = None

        if sheet_name_sold:
            ws_sold = wb[sheet_name_sold]
            headers_sold = [str(cell.value).strip() if cell.value is not None else "" for cell in ws_sold[1]]
            data_to_upload_sold = [headers_sold]
            
            if target_tab_name == "E車源":
                try:
                    target_gsheet_sold = doc.worksheet("E車源售出")
                    color_requests_sold.append({
                        "repeatCell": {
                            "range": { "sheetId": target_gsheet_sold.id, "startRowIndex": 1 },
                            "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": { "red": 1.0, "green": 1.0, "blue": 1.0 }}}},
                            "fields": "userEnteredFormat.backgroundColorStyle,userEnteredFormat.backgroundColor"
                        }
                    })
                except gspread.exceptions.WorksheetNotFound: pass
            
            for row in ws_sold.iter_rows(min_row=2):
                row_values = [cell.value if cell.value is not None else "" for cell in row]
                if not any(str(v).strip() for v in row_values): continue
                target_row_idx = len(data_to_upload_sold)
                if target_gsheet_sold:
                    for c_idx, cell in enumerate(row):
                        rgb = get_color_rgb(cell)
                        if rgb:
                            color_requests_sold.append({
                                "repeatCell": {
                                    "range": { "sheetId": target_gsheet_sold.id, "startRowIndex": target_row_idx, "endRowIndex": target_row_idx + 1, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1 },
                                    "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": { "red": rgb[0], "green": rgb[1], "blue": rgb[2] }}}},
                                    "fields": "userEnteredFormat.backgroundColorStyle"
                                }
                            })
                data_to_upload_sold.append(row_values)

        messages = []
        try:
            target_gsheet_main.clear()
            stringified_main = [[str(cell) if cell is not None else "" for cell in row] for row in data_to_upload_main]
            target_gsheet_main.update(values=stringified_main, range_name='A1')
            if target_tab_name == "新竹車源": target_gsheet_main.update_acell('A2', '="共"&SUMPRODUCT(--(LEN(TRIM($C$5:$C$133))>0))&"台"')
            doc.batch_update({"requests": color_requests_main})
            messages.append(f"「{target_tab_name}」成功({len(data_to_upload_main)-1}筆)")
        except Exception as e: return {"status": "error", "message": f"寫入主表失敗：{str(e)}"}
            
        if data_to_upload_sold and target_tab_name == "E車源" and target_gsheet_sold:
            try:
                target_gsheet_sold.clear()
                stringified_sold = [[str(cell) if cell is not None else "" for cell in row] for row in data_to_upload_sold]
                target_gsheet_sold.update(values=stringified_sold, range_name='A1')
                if len(color_requests_sold) > 1: doc.batch_update({"requests": color_requests_sold})
                messages.append(f"「E車源售出」成功({len(data_to_upload_sold)-1}筆)")
            except: messages.append("「E車源售出」寫入失敗")
        elif data_to_upload_sold and target_tab_name == "新竹車源":
            messages.append("已略過新竹已售")

        if target_tab_name == "E車源": load_and_clean_data()
            
        final_msg = " ＆ ".join(messages)
        if new_cars_msg_list:
            if len(new_cars_msg_list) > 20:
                new_cars_msg_list = new_cars_msg_list[:20]
                new_cars_msg_list.append("...(以下省略，新車數量較多)")
            final_msg += f"\n\n🎉 發現 {len(new_cars_msg_list)} 台新進車輛：\n" + "\n".join(new_cars_msg_list)

        return {"status": "success", "message": final_msg}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"處理失敗：{str(e)}"}

@app.post("/api/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    filename = file.filename
    contents = await file.read()
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
    
    if not filename.endswith('.xlsx'):
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 老闆，請上傳 .xlsx 格式的 Excel 檔案喔！"))
        return
    
    reply_msg = "⏳ 收到檔案！正在幫您解析資料與精準同步底色，請稍候...\n(處理完成後會自動回報)"
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_msg))
    
    def process_and_notify():
        try:
            message_content = line_bot_api.get_message_content(message_id)
            contents = b"".join([chunk for chunk in message_content.iter_content()])
            result = process_excel_file(filename, contents)
            if result["status"] == "success": line_bot_api.push_message(event.source.user_id, TextSendMessage(text="✅ 處理完成！\n" + result["message"]))
            else: line_bot_api.push_message(event.source.user_id, TextSendMessage(text="❌ 處理失敗：\n" + result["message"]))
        except Exception as e: line_bot_api.push_message(event.source.user_id, TextSendMessage(text=f"❌ 發生系統錯誤：\n{str(e)}"))

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
        TextSendMessage(text="🤖 您好！我是自動上傳小幫手。\n請直接將 Excel 檔案傳到這裡，我就會幫您自動同步！\n\n📝 記客資請輸入：\n客資 / 姓名 / 電話 / 找什麼車 / 備註")
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
