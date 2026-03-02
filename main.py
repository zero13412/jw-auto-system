from fastapi import FastAPI, Query, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import openpyxl
import gspread
from google.oauth2.service_account import Credentials
import re
import os
import io

app = FastAPI(title="🚗 杰運汽車新竹店 - 內部系統 API")

# 允許前端跨域請求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Google Sheet 設定
SHEET_ID = "1HWb5u6EGYSHVJHFhmhmsVv4xDgHlQEkdicfXBuFp86w"
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=0"

cached_df = None

def clean_money(val):
    if pd.isna(val): return 0.0
    s = str(val)
    matches = re.findall(r"(\d+\.?\d*)", s)
    if matches:
        try: return float(matches[-1])
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

    def normalize_property(row):
        p = str(row.get('產權', '')).strip()
        if p and p.lower() != 'nan': return p
        z = str(row.get('展帆', '')).strip()
        if z and z.lower() != 'nan': return z
        c = str(row.get('公司', '')).strip()
        if c and c.lower() != 'nan':
            if c == '杰': return '杰運' 
            return c
        return "其他"
    
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

# ================= API 區塊 =================

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
    sort_by: str = "預設", limit: int = 100
):
    if cached_df is None: load_and_clean_data()
    res = cached_df.copy()

    if brand != "全部": res = res[res['廠牌'] == brand]
    if location != "全部": res = res[res['車輛位置'] == location]
    if prop != "全部": res = res[res['filter_property'] == prop]
    
    if model and '車型' in res.columns: res = res[res['車型'].astype(str).str.lower().str.contains(model.lower(), na=False)]
    if version and '版本' in res.columns: res = res[res['版本'].astype(str).str.lower().str.contains(version.lower(), na=False)]
    if vin and '車身' in res.columns: res = res[res['車身'].astype(str).str.lower().str.contains(vin.lower(), na=False)]
    if plate and '車牌' in res.columns: res = res[res['車牌'].astype(str).str.lower().str.contains(plate.lower(), na=False)]
    if person and '負責人' in res.columns: res = res[res['負責人'].astype(str).str.lower().str.contains(person.lower(), na=False)]

    res = res[(res['顯示價格'] >= min_price) & (res['顯示價格'] <= max_price)]

    if sort_by == "價格低到高": 
        res = res.sort_values(by='顯示價格', ascending=True)
    elif sort_by == "價格高到低": 
        res = res.sort_values(by='顯示價格', ascending=False)
    elif sort_by == "年份舊到新":
        if '年份' in res.columns: 
            res['年份_num'] = pd.to_numeric(res['年份'], errors='coerce').fillna(0)
            res = res.sort_values(by='年份_num', ascending=True)
            res = res.drop(columns=['年份_num'])
    elif sort_by == "年份新到舊":
        if '年份' in res.columns: 
            res['年份_num'] = pd.to_numeric(res['年份'], errors='coerce').fillna(0)
            res = res.sort_values(by='年份_num', ascending=False)
            res = res.drop(columns=['年份_num'])
    elif sort_by == "入庫日新到舊":
        if '入庫_dt' in res.columns:
            res = res.sort_values(by='入庫_dt', ascending=False, na_position='last')
    elif sort_by == "入庫日舊到新":
        if '入庫_dt' in res.columns:
            res = res.sort_values(by='入庫_dt', ascending=True, na_position='last')
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

# ================= 自動處理 Excel 與上傳 API =================
@app.post("/api/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        wb = openpyxl.load_workbook(filename=io.BytesIO(contents), data_only=True)
        
        # 尋找上傳 Excel 內的指定工作表 (包容名稱前後有空白的狀況)
        sheet_name = None
        for name in wb.sheetnames:
            if "車源證件資料" in name:
                sheet_name = name
                break
        if not sheet_name:
            sheet_name = wb.sheetnames[0] 
        
        ws = wb[sheet_name]
        headers = [cell.value if cell.value is not None else "" for cell in ws[1]]
        
        col_model = headers.index("車型") if "車型" in headers else -1
        col_version = headers.index("版本") if "版本" in headers else -1
        
        if "收訂狀態" not in headers:
            headers.append("收訂狀態")
        status_idx = headers.index("收訂狀態")
        
        data_to_upload = [headers]
        
        for row in ws.iter_rows(min_row=2):
            row_values = [cell.value if cell.value is not None else "" for cell in row]
            
            # 判斷是否為全空行，全空行就跳過
            if not any(str(v).strip() for v in row_values):
                continue
                
            while len(row_values) < len(headers):
                row_values.append("")
            
            is_reserved = False
            
            # 判斷車型底色
            if col_model != -1 and row_values[col_model] != "":
                fill = row[col_model].fill
                if fill and fill.patternType and fill.start_color.rgb not in ['00000000', 'FFFFFFFF', None]:
                    is_reserved = True
            
            # 判斷版本底色
            if not is_reserved and col_version != -1 and row_values[col_version] != "":
                fill = row[col_version].fill
                if fill and fill.patternType and fill.start_color.rgb not in ['00000000', 'FFFFFFFF', None]:
                    is_reserved = True
                    
            row_values[status_idx] = "已收訂" if is_reserved else ""
            data_to_upload.append(row_values)
        
        # 尋找金鑰檔案
        key_path = "/etc/secrets/google_key.json"
        if not os.path.exists(key_path):
            return {"status": "error", "message": "尚未設定 Google API 憑證！"}

        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(key_path, scopes=scopes)
        client = gspread.authorize(creds)
        
        # 開啟 Google Sheet 檔案
        doc = client.open_by_key(SHEET_ID)
        
        # 【關鍵修正】：精準尋找線上 Google Sheet 內名為「車源證件資料表」的分頁
        target_gsheet = None
        for sheet in doc.worksheets():
            if "車源證件資料" in sheet.title:
                target_gsheet = sheet
                break
        
        if not target_gsheet:
            target_gsheet = doc.get_worksheet(0) # 如果真的找不到，才抓最左邊第一頁
            
        target_gsheet.clear()
        
        # 將所有資料轉換為字串避免 Google API 報錯
        stringified_data = [[str(cell) if cell is not None else "" for cell in row] for row in data_to_upload]
        
        # 將資料覆蓋進去
        target_gsheet.update(values=stringified_data, range_name='A1')
        
        # 同步更新網頁緩存
        load_and_clean_data()
        return {"status": "success", "message": f"成功同步 {len(data_to_upload)-1} 筆車源！包含底色標記。"}
        
    except Exception as e:
        return {"status": "error", "message": f"處理失敗：{str(e)}"}

# ================= 網頁路由區塊 =================
@app.get("/")
def serve_home(): return FileResponse("index.html")
@app.get("/cars")
def serve_cars(): return FileResponse("cars.html")
@app.get("/deal")
def serve_deal(): return FileResponse("deal.html")
@app.get("/loan")
def serve_loan(): return FileResponse("loan.html")
