* Mục tiêu:
Thiết kế và xây dựng Data Warehouse nhiều tầng (Landing – Staging – Atomic) nhằm tích hợp và chuẩn hóa dữ liệu tài chính từ nhiều nguồn khác nhau bằng SQL Server và Python.
Xây dựng quy trình ETL bằng Aiflow và Python để tự động trích xuất, làm sạch và tải dữ liệu vào kho dữ liệu, đảm bảo tính nhất quán và chất lượng dữ liệu.
Thiết kế mô hình dữ liệu đa chiều phục vụ phân tích các chỉ số tài chính như Balance Sheet, Cash Flow, và Business
Phát triển dashboard Power BI tương tác giúp trực quan hóa dữ liệu và phân tích hiệu quả kinh doanh theo nhiều chiều (thời gian, doanh nghiệp, chỉ tiêu tài chính).
Tích hợp xây dựng Chatbot OpenAPI kết nối hệ thống dữ liệu dành cho người không chuyên về SQL có thể tạo ra các câu truy vấn, vẽ biểu đồ cơ bản.
Tự động hóa cập nhật và phân phối báo cáo bằng Power BI Service và Power Automate, giúp giảm thời gian tổng hợp báo cáo thủ công và hỗ trợ ra quyết định nhanh hơn
* Kiến trúc DWH:
<img width="1083" height="498" alt="image" src="https://github.com/user-attachments/assets/e68d4631-dbf1-473b-8fb3-8519988d2f5c" />
* Quy trình ETL:
  <img width="1132" height="507" alt="image" src="https://github.com/user-attachments/assets/ab7a9b5c-2139-4271-9222-fd27925fe2da" />
<img width="1180" height="571" alt="image" src="https://github.com/user-attachments/assets/2a0268e6-4591-48e2-a2e8-80047f9df511" />
* Luồng xây dựng báo cáo trên Power BI:
  <img width="1246" height="353" alt="image" src="https://github.com/user-attachments/assets/bf9957c5-155c-4bf6-96be-2c6157e985af" />
- Các DAG được xây dựng và lập lịch:
  <img width="1181" height="499" alt="image" src="https://github.com/user-attachments/assets/af441915-5008-4694-9af3-8d295569aace" />
* FLow gửi dashboard thành PDF qua email định kỳ:
  <img width="1251" height="616" alt="image" src="https://github.com/user-attachments/assets/84f2953d-4f25-42cb-8ae8-a0d39f02484d" />


