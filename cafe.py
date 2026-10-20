import os
import json
import pandas as pd
import requests
from datetime import datetime
import re
from playwright.sync_api import sync_playwright

# 1. Khởi tạo cấu trúc thư mục local
BASE_DIR = "data/raw"
os.makedirs(f"{BASE_DIR}/prices", exist_ok=True)
os.makedirs(f"{BASE_DIR}/weather", exist_ok=True)

# 2. Hàm cào dữ liệu giá nông sản (Web Scraping)
def extract_coffee_prices():
    print("Đang khởi động trình duyệt ảo lấy dữ liệu giá cà phê...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50) 
        
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        try:
            page.goto("https://giacaphe.com/gia-ca-phe-noi-dia/", timeout=60000, wait_until="domcontentloaded")
            page.wait_for_selector("table.price-table", timeout=15000)
            
            # Đợi CSS load hoàn tất
            page.wait_for_timeout(3000)
            
            # Dùng JavaScript đọc trực tiếp vào CSS ::after
            rows_data = page.evaluate('''() => {
                const rows = document.querySelectorAll('table.price-table tbody tr');
                const data = [];
                rows.forEach(row => {
                    const cols = row.querySelectorAll('td');
                    if(cols.length >= 2) {
                        let priceText = "";
                        // Tìm thẻ span chứa class mã hóa bên trong cột giá
                        const span = cols[1].querySelector('span');
                        
                        if (span) {
                            // Đọc thuộc tính content của ::after
                            const afterContent = window.getComputedStyle(span, '::after').getPropertyValue('content');
                            if (afterContent && afterContent !== 'none') {
                                // Xóa dấu nháy kép bọc ngoài chuỗi giá trị (ví dụ: '"94,000"' -> '94,000')
                                priceText = afterContent.replace(/['"]/g, ''); 
                            }
                        }
                        
                        // Fallback (dự phòng) trường hợp trang web tắt chế độ chống cào
                        if (!priceText) {
                            priceText = cols[1].innerText.trim();
                        }

                        data.push({
                            province: cols[0].innerText.trim(),
                            price_raw: priceText
                        });
                    }
                });
                return data;
            }''')
            
            data = []
            valid_provinces = ['Đắk Lắk', 'Lâm Đồng', 'Gia Lai', 'Đắk Nông', 'Kon Tum', 'Đồng Nai']
            
            for item in rows_data:
                province = item['province']
                if any(vp in province for vp in valid_provinces):
                    digits_only = re.sub(r'\D', '', item['price_raw'])
                    if digits_only and len(digits_only) >= 4:
                        data.append({
                            "province": province,
                            "price_vnd": int(digits_only),
                            "extracted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
            
            if data:
                df = pd.DataFrame(data)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                file_path = f"{BASE_DIR}/prices/coffee_price_{timestamp}.csv"
                df.to_csv(file_path, index=False, encoding='utf-8-sig')
                print(f"✅ Đã lưu file giá cà phê thành công tại: {file_path}")
                print(df.head())
            else:
                print("⚠️ Dữ liệu thô cào được vẫn bị rỗng. Chi tiết data thô:")
                print(rows_data)
                
        except Exception as e:
            print(f"❌ Lỗi khi scraping bằng Playwright: {e}")
        finally:
            browser.close()

# 3. Hàm lấy dữ liệu thời tiết qua API
def extract_weather_data():
    print("Đang lấy dữ liệu thời tiết...")
    API_KEY = "badd5ce0765c1958a500eac58b122562" 
    CITY = "Dak Lak"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        data['extracted_at'] = datetime.now().isoformat()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        file_path = f"{BASE_DIR}/weather/weather_daklak_{timestamp}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"✅ Đã lưu file thời tiết tại: {file_path}")
        
    except Exception as e:
        print(f"❌ Lỗi khi lấy API thời tiết: {e}")

# 4. Chạy các tiến trình
if __name__ == "__main__":
    print(f"Bắt đầu quy trình Extract (Local) lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    extract_coffee_prices()
    extract_weather_data()
    
    print("-" * 50)
    print("Hoàn tất!")