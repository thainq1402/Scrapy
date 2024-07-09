# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector

class ImportToMySQL:
    
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = '103.200.22.212',
            port = '3306',
            user = 'dulieutu',
            password = ':EHr0H1o5.Pro2',
            database = 'dulieutu_TTTuyenDung'
        )
        # self.conn = mysql.connector.connect(
        #     host = '127.0.0.1',
        #     port = '3306',
        #     user ='root',
        #     password = 'Camtruykich123',
        #     database = 'tuyendung'
        # )
        ## Create cursor, used to execute commands
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        sql = """
            REPLACE INTO Stg_ThongTin(Web, Nganh, Link, TenCV, CongTy, TinhThanh, Luong, LoaiHinh, KinhNghiem, CapBac, HanNopCV, YeuCau, MoTa, PhucLoi, SoLuong) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.cur.execute(sql, (item['Web'], item['Nganh'], item['Link'], item['TenCV'], item['CongTy'], item['TinhThanh'], item['Luong'], item['LoaiHinh'], item['KinhNghiem'], item['CapBac'], item['HanNopCV'], item['YeuCau'], item['MoTa'], item['PhucLoi'], item['SoLuong']))
        self.conn.commit()

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()