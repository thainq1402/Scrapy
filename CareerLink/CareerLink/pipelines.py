# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector

# Pipeline tiền xử lý dữ liệu trước khi đưa vào cơ sở dữ liệu

class CleanItem:

    def process(self, item, spider):
        adapter = ItemAdapter(item)
        
        field_key = ['Nganh', 'TinhThanh', 'MoTa', 'YeuCau', 'PhucLoi']
        # Nối lại thành 1 chuỗi do các trường trên là các mảng
        for key in field_key:
            value = adapter.get(key)
            change = ""
            for i in range(len(value)):
                if value[i] == '\n':
                    continue
                change += value[i]
            adapter[key] = change
        return item


class ImportToMySQL:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = '103.56.158.31',
            port = '3306',
            user = 'tuyendungUser',
            password = 'sinhvienBK',
            database = 'ThongTinTuyenDung'
        )
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        sql = """
            INSERT IGNORE INTO Stg_Data_Raw (Web, Nganh, Link, TenCV, CongTy, TinhThanh, Luong, LoaiHinh, KinhNghiem, CapBac, HanNopCV, YeuCau, MoTa, PhucLoi, SoLuong, Image) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.cur.execute(sql, (item['Web'], item['Nganh'], item['Link'], item['TenCV'], item['CongTy'], item['TinhThanh'], item['Luong'], item['LoaiHinh'], item['KinhNghiem'], item['CapBac'], item['HanNopCV'], item['YeuCau'], item['MoTa'], item['PhucLoi'], item['SoLuong'], item['Img']))
        self.conn.commit()

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
        
class DatabaseConnector:
    def __init__(self, host, user, port, password, database):
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.database = database

    def connect(self):
        return mysql.connector.connect(
            host=self.host,
            port = self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    # Lấy link trong DB để check trùng lặp
    def get_links_from_database(self):
        connection = self.connect()
        cursor = connection.cursor()

        query = "SELECT Link FROM Fact WHERE ID_Web ='11'"
        cursor.execute(query)

        links = [row[0] for row in cursor.fetchall()]

        cursor.close()
        connection.close()

        return links