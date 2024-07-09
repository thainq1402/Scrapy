from typing import Iterable
import scrapy
from scrapy.http import Request
import math
from bs4 import BeautifulSoup
import json
import requests
from ITNaVi.items import ITNaVi
from ITNaVi.pipelines import DatabaseConnector
import re
import html

def decode_special_string(input_str):
    # Tìm các chuỗi Unicode và giải mã
    unicode_matches = re.finditer(r'\\u([0-9a-fA-F]{4})', input_str)
    decoded_str = input_str
    for match in unicode_matches:
        unicode_str = match.group(0)
        unicode_char = chr(int(unicode_str[2:], 16))
        decoded_str = decoded_str.replace(unicode_str, unicode_char)

    # Giải mã các ký tự HTML
    decoded_str = html.unescape(decoded_str)

    return decoded_str

class ItvaviSpider(scrapy.Spider):
    name = "itnavi"
    allowed_domains = ["itnavi.com.vn"]
    
    def start_requests(self):
        # db_connector = DatabaseConnector(host='127.0.0.1', port = 3306, user='root', password='Camtruykich123', database='tuyendung_2')
        db_connector = DatabaseConnector(host='103.56.158.31', port = 3306, user='tuyendungUser', password='sinhvienBK', database='ThongTinTuyenDung')
        remove_url_list_local = db_connector.get_links_from_database()
        self.remove_url_list = remove_url_list_local
        print("Số lượng url trong CSDL: ", len(self.remove_url_list))
        yield scrapy.Request("https://itnavi.com.vn/job?", callback = self.parse)
        
    def parse(self, response):
        text = response.css('.js-tab__show h1::text').get()
        number_cv = ''.join(filter(str.isdigit, text))
        cv_count = int(number_cv)
        if cv_count % 10 == 0:
            max_page = int(cv_count / 10)
        else:
            max_page = math.floor(cv_count/10) + 1
        #**************************************************************************
        for page_number in range(1, max_page+1):
            page_url = "https://itnavi.com.vn/job?page=" + str(page_number)
            yield scrapy.Request(page_url, callback = self.id_parse)
            
    def id_parse(self, response):
        jobs_id_list = response.css('.jsl-list .jsl-item::attr(data-id)').extract()
        for job_id in jobs_id_list:
            job_url_by_id = "https://itnavi.com.vn/ajax/get-job-by-id/" + job_id
            yield scrapy.Request(job_url_by_id, callback = self.it_parse)
    
    def it_parse(self, response):
        data_json = response.json()
        #**************************************************************************
        item = ITNaVi()
        ID = "IT_NV_" + str(data_json["data"]["job_id"])
        Web = "ITNaVi"
        Nganh = "IT"
        Link = data_json["data"]["job_slug"]
        TenCV = data_json["data"]["job_name"]
        CongTy = data_json["data"]["company_name"]
        TinhThanh = data_json["data"]["job_addresses"]
        Luong = data_json["data"]["job_salary"]
        LoaiHinh = data_json["data"]["job_career_type"]
        KinhNghiem = "Không có"
        CapBac = data_json["data"]["job_career_level"]
        item['ID'] = ID
        item['Web'] = Web
        item['Nganh'] = Nganh
        item['Link'] = Link
        item['TenCV'] = TenCV
        item['CongTy'] = CongTy
        item['TinhThanh'] = TinhThanh
        item['Luong'] = Luong
        item['LoaiHinh'] = LoaiHinh
        item['KinhNghiem'] = KinhNghiem
        item['CapBac'] = CapBac
        if Link in self.remove_url_list:
            print("Trùng lặp: ", Link)
            return
        else:
            yield scrapy.Request(Link, method = 'GET', callback = self.it_parse_2, meta = {'item': item, 'data_json': data_json})
        #**************************************************************************
        
    def it_parse_2(self, response):
        HanNopCV = response.text.split("\"validThrough\": \"")[1].split("\"")[0]
        item = response.meta.get('item')
        data_json = response.meta.get('data_json')
        #**************************************************************************
        soup = BeautifulSoup(response.text, "html.parser")
        icon_i = soup.find("i", class_="fas fa-users")
        if icon_i:
            sibling_p = icon_i.find_next_sibling("p")
            if sibling_p:
                SoLuong = sibling_p.get_text(strip=True)
            else:
                SoLuong = "1"
        else:
            SoLuong = "1"
        #**************************************************************************
        root = data_json["data"]["job_content"]
        soup = BeautifulSoup(root, 'html.parser')
        cleaned_root = soup.get_text(separator='').lower()
        #**************************************************************************
        split_1 = ["\nyêu cầu:\n", "\nyêu cầu công việc\n"]
        split_2 = ["\nchế độ:\n", "\ncác phúc lợi dành cho bạn\n", "\ntại sao bạn sẽ yêu thích làm việc tại đây\n"]
        check = False
        split_check_2 = ""
        for split_string_1 in split_1:
            if split_string_1 in cleaned_root:
                MoTa = cleaned_root.split(split_string_1)[0]
                cleaned_root_TG = decode_special_string(cleaned_root.split(split_string_1)[1])
        for split_string_2 in split_2:
            if split_string_2 in cleaned_root_TG:
                split_check_2 += split_string_2
                check = True
        if check == True:
            YeuCau = decode_special_string(cleaned_root_TG.split(split_check_2)[0])
            PhucLoi = decode_special_string(cleaned_root_TG.split(split_check_2)[1])
        else:
            YeuCau = cleaned_root_TG
            PhucLoi = ""
        #**************************************************************************
        item['HanNopCV'] = HanNopCV
        item['SoLuong'] = SoLuong
        item['YeuCau'] = YeuCau
        item['MoTa'] = MoTa
        item['PhucLoi'] = PhucLoi
        
        yield item