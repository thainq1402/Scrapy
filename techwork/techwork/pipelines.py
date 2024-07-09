import mysql.connector
class SaveToMySQL_test_Pipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host='103.56.158.31',
            port='3306',
            user='tuyendungUser',
            password='sinhvienBK',
            database='ThongTinTuyenDung'
        )
        
        # self.conn = mysql.connector.connect(
        #     host='127.0.0.1',
        #     port='3306',
        #     user='root',
        #     password='Camtruykich123',
        #     database='tuyendung_2'
        # )

        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        sql = """
            INSERT IGNORE INTO Stg_Data_Raw (Web, Nganh, Link, TenCV, CongTy, TinhThanh, Luong, LoaiHinh, KinhNghiem, CapBac, HanNopCV, YeuCau, MoTa, PhucLoi, SoLuong) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.cur.execute(sql, (item['Web'], item['Nganh'], item['Link'], item['TenCV'], item['CongTy'], item['TinhThanh'], item['Luong'], item['LoaiHinh'], item['KinhNghiem'], item['CapBac'], item['HanNopCV'], item['YeuCau'], item['MoTa'], item['PhucLoi'], item['SoLuong']))
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

    def get_links_from_database(self):
        connection = self.connect()
        cursor = connection.cursor()

        query = "SELECT Link FROM Fact WHERE ID_Web ='13'"
        cursor.execute(query)

        links = [row[0] for row in cursor.fetchall()]

        cursor.close()
        connection.close()

        return links
