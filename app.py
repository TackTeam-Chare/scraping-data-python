import pandas as pd
import mysql.connector

# โหลดไฟล์ CSV
file_path = 'google_1.csv'
data = pd.read_csv(file_path)

# เลือกคอลัมน์ที่เกี่ยวข้องและเปลี่ยนชื่อคอลัมน์ให้เข้าใจง่ายขึ้น
relevant_columns = data.iloc[2:, [2, 4, 5, 7, 8, 9, 10]]
relevant_columns.columns = ['name', 'rating', 'reviews', 'location', 'opening_status', 'location_code', 'opening_hours']

# แยกสถานะการเปิด-ปิดเวลา
relevant_columns['opening_hours'] = relevant_columns['opening_hours'].str.extract(r'⋅ ปิด (\d{2}:\d{2})')

# ลบคอลัมน์ที่ไม่จำเป็น
cleaned_data = relevant_columns[['name', 'location', 'location_code', 'opening_hours']].copy()

# แยกละติจูดและลองจิจูดจาก location_code
cleaned_data['latitude'] = cleaned_data['location_code'].str.extract(r'(\d{4})\+')[0]
cleaned_data['longitude'] = cleaned_data['location_code'].str.extract(r'\+(\w{4})')[0]

# ลบคอลัมน์ location_code
cleaned_data.drop(columns=['location_code'], inplace=True)

# เพิ่มคอลัมน์ description, district, และ category
cleaned_data['description'] = None
cleaned_data['district'] = 1  # ตัวอย่าง district_id
cleaned_data['category'] = 9  # ร้านอาหาร

# จัดลำดับคอลัมน์ให้ตรงกับโครงสร้างฐานข้อมูล
final_data = cleaned_data[['name', 'description', 'location', 'latitude', 'longitude', 'district', 'category', 'opening_hours']]

# ฟังก์ชันสำหรับการสร้างค่า SQL
def format_sql_value(value):
    if pd.isna(value) or value is None:
        return "NULL"
    else:
        return f"'{value}'"

# สร้างคำสั่ง SQL INSERT INTO
sql_statements_entities = []
sql_statements_hours = []
created_by = 1   # User id ที่สร้างข้อมูลนี้

for index, row in final_data.iterrows():
    if pd.notna(row['name']):  # ข้ามแถวที่ชื่อเป็น null
        sql_entity = f"INSERT INTO tourist_entities (name, description, location, latitude, longitude, district_id, category_id, created_by, created_date) VALUES ({format_sql_value(row['name'])}, {format_sql_value(row['description'])}, {format_sql_value(row['location'])}, {format_sql_value(row['latitude'])}, {format_sql_value(row['longitude'])}, {row['district']}, {row['category']}, {created_by}, NOW());"
        sql_statements_entities.append(sql_entity)

        if 'opening_hours' in final_data.columns and pd.notna(row['opening_hours']):
            place_name = format_sql_value(row['name'])
            sql_hours = f"""
            INSERT INTO operating_hours (place_id, day_of_week, opening_time, closing_time)
            SELECT id, 'Monday', '08:00:00', {format_sql_value(row['opening_hours'])}
            FROM tourist_entities
            WHERE name = {place_name}
            LIMIT 1;
            """
            sql_statements_hours.append(sql_hours)

# ตั้งค่าการเชื่อมต่อฐานข้อมูล
config = {
    'user': 'root',       # ใส่ชื่อผู้ใช้ที่ถูกต้อง
    'password': '',   # ใส่รหัสผ่านที่ถูกต้อง
    'host': 'localhost',           # โฮสต์ของฐานข้อมูล
    'database': 'place',           # ชื่อฐานข้อมูล
    'raise_on_warnings': True
}

# สร้างการเชื่อมต่อและรันคำสั่ง SQL
try:
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    
    for sql in sql_statements_entities:
        cursor.execute(sql)
    
    connection.commit()

    for sql in sql_statements_hours:
        cursor.execute(sql)
    
    connection.commit()
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    cursor.close()
    connection.close()
