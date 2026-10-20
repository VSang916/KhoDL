# Kết quả phân tích & học máy - giá rau quả trong nước

Nguồn: `ttns.v_price_monthly` (ClickHouse), thitruongnongsan.gov.vn, 04/2021 → 12/2025, đơn vị VNĐ/kg.
Notebook: `phan_tich_gia_nong_san.ipynb` (mã nguồn dạng `.py` cùng tên) · biểu đồ: `figures/` · bảng số: `outputs/`.
Chạy ngày 19/10/2026; dữ liệu chụp lại trong `data/ttns_v_price_monthly.csv` để chạy lại cho cùng kết quả.

---

## 1. Dữ liệu

| Chỉ tiêu | Giá trị |
|---|---|
| Khoảng thời gian | 04/2021 → 12/2025 (57 tháng) |
| Số dòng (mặt hàng × tỉnh × tháng × loại giá) | 6.925 |
| Số lần báo giá gốc | 38.341 |
| Sản phẩm / mặt hàng / tỉnh | 73 / 188 / 14 |
| Số chuỗi giá | 429 - trung bình 16,1 tháng; 210 chuỗi ≥ 12, 126 ≥ 24, 49 ≥ 36 tháng |

**Biến động tháng** (`figures/02_bien_dong_thang.png`)

| Cấp giá | Thay đổi trung vị | 10% trường hợp đổi hơn | Tăng > 2% | Giảm > 2% |
|---|---|---|---|---|
| Bán lẻ | 5,9% | 33% | 27,5% | 26,1% |
| Thu mua | 7,1% | 33% | 36,6% | 32,8% |
| Bán buôn | 6,1% | 30% | 28,7% | 34,2% |

→ Giá thu mua tại vườn biến động mạnh hơn giá bán lẻ; khoảng 1/3 số tháng giá đứng yên (đổi ≤ 2%).

## 2. Mùa vụ

**Chỉ số mùa vụ** = giá tháng so với giá trung bình của chính năm đó, trung bình qua các năm (`figures/03_heatmap_mua_vu.png`,
`outputs/mua_vu_san_pham.csv`). Một số sản phẩm dao động mạnh nhất:

| Sản phẩm | Rẻ nhất | Đắt nhất | Biên độ |
|---|---|---|---|
| Chôm chôm | T7 (−21%) | T4 (+37%) | 58 điểm % |
| Bắp cải | T2 (−31%) | T12 (+26%) | 57 |
| Củ cải | T2 (−18%) | T12 (+33%) | 51 |
| Khổ qua | T2 (−17%) | T12 (+29%) | 46 |
| Sầu riêng | T6 (−19%) | T1 (+23%) | 42 |
| Xoài | T5 (−23%) | T1 (+18%) | 41 |
| Thanh long | T8 (−20%) | T4 (+20%) | 40 |
| Cà chua | T4 (−18%) | T12 (+21%) | 38 |

**Phân cụm KMeans** trên hình dạng 12 tháng của 169 chuỗi (`figures/04_cum_mua_vu.png`). Chọn k = 4 theo silhouette
(0,207 - cấu trúc cụm yếu, nên đọc như xu hướng chứ không phải nhóm tách bạch):

| Cụm | Số chuỗi | Hình dạng | Tiêu biểu |
|---|---|---|---|
| 1 | 22 | đắt T1-T4, **rẻ sâu T6-T7** (vào vụ chính), biên độ 56% | sầu riêng, chôm chôm, xoài, mướp |
| 2 | 21 | đỉnh T3-T4, thấp T8-T12, biên độ 40% | cam, thanh long, nhãn, chanh |
| 3 | 31 | **thấp T2-T5, tăng dần tới đỉnh T12** (cận Tết), biên độ 36% | bắp cải, cà chua, củ cải, dưa chuột, khổ qua |
| 4 | 95 | gần như **không có mùa vụ** (biên độ 8%) | bưởi, dưa hấu, cam, xoài, nhãn |

→ Rau ăn lá / ăn quả (cụm 3) đắt nhất dịp cận Tết; trái cây nhiệt đới (cụm 1) rẻ nhất giữa năm khi vào vụ.
Hơn một nửa số chuỗi (cụm 4) không có mùa vụ rõ - nên mùa vụ một mình không đủ để dự báo.

## 3. Dự báo giá

**Thiết kế:** backtest rolling-origin - tại mỗi tháng gốc chỉ dùng dữ liệu đến tháng đó; 12 gốc kiểm tra 12/2024 → 11/2025;
12 gốc trước đó (12/2023 → 11/2024) dùng để hiệu chỉnh khoảng giá. So trên **cùng một tập điểm** cho mọi mô hình
(765 điểm, 100 chuỗi có ≥ 24 tháng).

### 3.1 Tháng tới (h = 1) - `figures/05_so_sanh_mo_hinh.png`

| Phương pháp | MAPE | APE trung vị | So với naive |
|---|---|---|---|
| **Naive (giá tháng gốc)** | **14,4%** | 7,1% | 1,00× |
| Ensemble (naive + AutoETS + LightGBM) | 14,9% | 8,2% | 1,03× |
| AutoTheta | 15,5% | 9,1% | 1,08× |
| AutoETS | 15,6% | 9,1% | 1,08× |
| LightGBM (global) | 15,7% | 8,5% | 1,09× |
| Trung bình 3 tháng | 18,5% | 10,7% | 1,28× |
| Seasonal naive (cùng tháng năm trước) | 30,8% | 18,4% | 2,13× |

→ Ở tầm 1 tháng **không mô hình nào thắng "giữ nguyên giá tháng này"**: giá tháng kế tiếp gần như bước ngẫu nhiên.
Seasonal naive kém nhất vì mặt bằng giá năm nay khác năm trước nhiều.

### 3.2 Theo tầm dự báo - `figures/06_sai_so_theo_tam_du_bao.png`

| h (tháng) | Naive | AutoETS | **LightGBM** | Ensemble |
|---|---|---|---|---|
| 1 | **14,4%** | 15,6% | 15,7% | 14,9% |
| 2 | 21,6% | 21,9% | 21,3% | **21,2%** |
| 3 | 26,0% | 25,8% | **24,1%** | 24,8% |
| 4 | 28,9% | 28,1% | **25,6%** | 27,1% |
| 5 | 29,5% | 28,6% | **26,3%** | 27,7% |
| 6 | 28,4% | 27,7% | **25,3%** | 26,6% |

→ **Từ 3 tháng trở đi LightGBM tốt nhất, giảm sai số ~7-11% so với naive.** Ở tầm xa, thông tin mùa vụ
(tháng đích, giá 6/12 tháng trước, biến động năm trước) bắt đầu có ích. Điều này có giá trị thực tế vì nguồn
công bố chậm khoảng 10 tháng: muốn ước lượng giá hiện tại phải dự báo xa.

Theo cấp giá (h = 1): bán lẻ naive 13,7% / LightGBM 15,3%; thu mua 15,0% / 16,0%; bán buôn (24 điểm) AutoETS tốt nhất 14,3%.

### 3.3 Khoảng giá 80% - `figures/07_vi_du_du_bao.png`, `outputs/khoang_gia_80.csv`

| h | LightGBM + conformal: độ phủ / độ rộng | Naive + conformal | AutoETS (khoảng sẵn có) |
|---|---|---|---|
| 1 | 78% / 39% | 78% / 38% | 73% / 42% |
| 3 | 75% / 61% | 78% / 67% | 69% / 64% |
| 6 | 74% / 65% | 71% / 70% | 72% / 86% |

→ Khoảng conformal (lấy phân vị sai số thật trên tập hiệu chỉnh) đạt độ phủ 74-78%, sát mục tiêu 80%;
khoảng có sẵn của AutoETS phủ thiếu (69-73%) và rộng hơn ở tầm xa. Độ rộng ~40% ở h = 1 phản ánh đúng mức biến động thật.

### 3.4 LightGBM dựa vào gì - `figures/08_do_quan_trong_feature.png`

Sản phẩm (28%) · giá 6 tháng trước (7,5%) · tuổi chuỗi (7,5%) · giá 12 tháng trước (7,3%) · tháng đích (6,7% + 3,8%) ·
độ biến động 6 tháng (6,0%) · giá 3 tháng trước (5,9%) · tỉnh (5,3%) · biến động cùng kỳ năm trước (4,6%).
→ Mô hình học "mỗi sản phẩm có nhịp riêng theo tháng"; % đổi của tháng vừa qua gần như không giúp (0,9%).

### 3.5 Dự đoán chiều giá tháng tới - `figures/09_ma_tran_nham_lan.png`

Nhãn tăng (> +2%) / giảm (< −2%) / đi ngang; 1.780 điểm, phân bố thật: ngang 36%, tăng 34%, giảm 30%.

| | Accuracy | Macro-F1 |
|---|---|---|
| **LGBMClassifier** | **47,6%** | **46,6%** |
| Luôn đoán lớp phổ biến | 36,1% | 17,7% |

→ Đoán **chiều** giá tốt hơn hẳn đoán bừa (+11,5 điểm accuracy), dù đoán **mức** giá tháng tới không thắng naive.
Nhận diện "đi ngang" tốt nhất (382/642 đúng); nhầm nhiều giữa tăng và giảm.

## 4. Giá bất thường - `figures/10_gia_bat_thuong.png`, `outputs/gia_bat_thuong.csv`

Robust z-score của % đổi giá tháng (median / MAD của chính chuỗi, MAD sàn 9,1%); bất thường khi |z| > 4 và đổi > 30%.
**91 điểm / 6.083 lần đổi giá (1,5%)**, ở 62 chuỗi; 56 điểm là giá thu mua, 32 bán lẻ.
Lớn nhất: nhãn tiêu da bò thu mua Vĩnh Long 08/2025 (−74%), bắp cải thảo bán lẻ Sơn La 02/2025 (−75%),
nhãn thu mua Cần Thơ 05/2022 (+229%).
→ Phần lớn là giá thu mua sụp/tăng vọt theo vụ; một số có thể do nhập liệu - danh sách dùng để kiểm tra lại với nguồn.

## 5. Chênh lệch bán lẻ - thu mua - `figures/11_chenh_lech_le_thu_mua.png`, `outputs/chenh_lech_le_thu_mua.csv`

Ở mức sản phẩm (801 tháng-sản phẩm có cả 2 giá; 24 sản phẩm có ≥ 12 tháng):
giá bán lẻ / giá thu mua cao nhất ở củ cải 3,7×, ổi 3,5×, bưởi 3,0×, chanh 2,9×; thấp nhất chuối 1,2×, mít 1,0×, bắp cải 1,0×.

**Độ trễ truyền dẫn:** tương quan giữa % đổi giá thu mua tháng t−k và % đổi giá lẻ tháng t, trung bình 20 sản phẩm:
k = 0: +0,05 · k = 1: +0,07 · k = 2: +0,01 · k = 3: −0,06 → **gần như không có liên hệ**.

## Lưu ý khi diễn giải

- Giá lẻ đo chủ yếu ở miền Bắc (Hà Nội, Sơn La), giá thu mua ở miền Tây → hệ số ở mục 5 gồm cả vận chuyển và khác giống;
  hệ số ≈ 1 (mít, bắp cải) nhiều khả năng do so khác giống / khác loại, không phải không có biên trung gian.
- Không có liên hệ ở mục 5 cũng vì 2 giá đến từ 2 thị trường khác nhau, không phải bằng chứng giá lẻ không phụ thuộc giá vườn.
- Chuỗi ngắn (trung bình 16 tháng) và có tháng trống; mô hình từng chuỗi (ETS/Theta) chỉ áp dụng được cho 100-104 chuỗi ≥ 24 tháng.
- Không dùng mạng nơ-ron (LSTM/Transformer): ~7.000 điểm là quá ít.

## Đề xuất

1. **Dùng trong vận hành:** naive cho tháng tới, LightGBM cho tầm ≥ 3 tháng (ước lượng các tháng nguồn chưa công bố),
   luôn kèm khoảng 80% conformal; chọn lại mô hình mỗi lần có tháng mới bằng backtest như trên.
2. **Để dự báo tốt hơn:** thêm dữ liệu ngoài - lịch Tết âm lịch, thời tiết theo tỉnh (mưa, bão, hạn mặn), xuất khẩu sang
   Trung Quốc (mục 7 / mục 9 của cùng trang), giá xăng.
3. **Winmart:** tích luỹ vài tháng dữ liệu hằng ngày rồi phân tích thời gian khuyến mãi và so giá siêu thị - chợ.
