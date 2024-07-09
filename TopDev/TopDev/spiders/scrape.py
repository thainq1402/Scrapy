from typing import Iterable
import numpy as np
import scrapy
import json
from scrapy.http import Request
from TopDev.items import IT_Item
from bs4 import BeautifulSoup
from TopDev.pipelines import DatabaseConnector
from datetime import date

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


class ScrapeSpider(scrapy.Spider):
    name = "scrape"
    allowed_domains = ["topdev.vn"]
    
    def start_requests(self):
        # db_connector = DatabaseConnector(host='127.0.0.1', port = 3306, user='root', password='Camtruykich123', database='tuyendung_2')
        db_connector = DatabaseConnector(host='103.56.158.31', port = 3306, user='tuyendungUser', password='sinhvienBK', database='ThongTinTuyenDung')
        remove_url_list_local = db_connector.get_links_from_database()
        self.remove_url_list = remove_url_list_local
        print("Số lượng url trong CSDL: ", len(self.remove_url_list))
        for page_number in range(1, 100):
            url = f'https://api.topdev.vn/td/v2/jobs?fields[job]=id,slug,title,salary,company,extra_skills,skills_str,skills_arr,skills_ids,job_types_str,job_levels_str,job_levels_arr,job_levels_ids,addresses,status_display,detail_url,job_url,salary,published,refreshed,applied,candidate,requirements_arr,packages,benefits,content,features,is_free,is_basic,is_basic_plus,is_distinction&fields[company]=slug,tagline,addresses,skills_arr,industries_arr,industries_str,image_cover,image_galleries,benefits&page={page_number}&locale=vi_VN&ordering=jobs_new'
            yield scrapy.Request(url, method = 'GET', callback = self.parse)
            
    def parse(self, response):
        data = response.json()

        if len(data['data']) != 0:
            for cv_count in range(len(data['data'])):
                ID = "IT_TD_" + str(data['data'][cv_count]['id']) #Xử lí ID
                Web = "TopDev" #Xử lí trang web
                Nganh = "IT" #Tên ngành
                Link = data['data'][cv_count]['detail_url'] #Link công việc
                TenCV = data['data'][cv_count]['title'] #Tên công việc
                CongTy = data['data'][cv_count]['company']['display_name'] #Tên công ty
                TinhThanh = data['data'][cv_count]['addresses']['address_region_list']  #Địa điểm
                if data['data'][cv_count]['salary']['value'] == "":              #Lương
                    Luong = "Thương lượng"
                else:
                    Luong = "Pending"
                LoaiHinh = data['data'][cv_count]['job_types_str']                                   #Loại hình
                KinhNghiem = "Pending"                                  #Kinh nghiệm chưa có
                CapBac = data['data'][cv_count]['job_levels_str']       #Cấp bậc
                HanNopCV = date.today()                                   #Hạn nộp CV
                YeuCau = ""
                for requiment in data['data'][cv_count]['requirements_arr']:            #Yêu cầu
                    if (type(requiment['value']) == list):
                        for requiment_TG in requiment['value']:
                            requiment_TG = decode_special_string(requiment_TG)
                            YeuCau = YeuCau + requiment_TG + "\n"
                    else:
                            requiment_TG = decode_special_string(requiment['value'])
                            YeuCau = YeuCau + requiment_TG + "\n"
                MoTa = "Pending"                    #Mô tả chưa có
                PhucLoi = ""
                if len(data['data'][cv_count]['company']['benefits']) == 0:
                    pl = data['data'][cv_count]['benefits']
                else:
                    pl = data['data'][cv_count]['company']['benefits']
                for benefit in pl:                     #Phúc lợi
                    if type(benefit['value']) == list:
                        for benefit_TG in benefit['value']:
                            benefit_TG = decode_special_string(benefit_TG)
                            PhucLoi = PhucLoi + benefit_TG + "\n"
                    else:
                            benefit_TG = decode_special_string(benefit['value'])
                            PhucLoi = PhucLoi + benefit_TG + "\n"
                SoLuong = "1"
                item = IT_Item()
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
                item['YeuCau'] = YeuCau
                item['MoTa'] = MoTa
                item['PhucLoi'] = PhucLoi
                item['HanNopCV'] = HanNopCV
                item['SoLuong'] = SoLuong
                if Link in self.remove_url_list:
                    print("Trùng lặp: ", Link)
                    continue
                else:
                    yield scrapy.Request(Link, callback = self.parse_2, meta = {"my_item": item})
        else:
            return
    
    def parse_2(self, response):
        item = response.meta['my_item']
        script = response.css('div script')[1].extract()
        script = script.split("{")
        #********************************
        check = []
        for i in range(len(script)):
            u = 0
            if "minValue" in script[i]:
                u += 1
            if "maxValue" in script[i]:
                u += 1
            if "value" in script[i]:
                u += 1
            check.append(u)
        check_max = check.index(max(check))
        script_after = script[check_max].split("}}")
        for j in range(len(script_after)):
            if "value" in script_after[j]:
                script_after_2 = script_after[j].split(",")
                break
        for k in range(len(script_after_2)):
            if "value" in script_after_2[k]:
                if item['Luong'] == "Pending":
                    Luong = script_after_2[k].split(":")[1].replace("\"", "")                                   #Lương 
                    item['Luong'] = Luong
        #************************************************************
        for i in range(len(script)):
            if "monthsOfExperience" in script[i]:
                script_after = script[i].split("}}")
                KinhNghiem = script_after[0].split(":")[-1].replace("\"", "").strip() + " tháng"
                break
            else:
                KinhNghiem = "Không yêu cầu"    
        item['KinhNghiem'] = KinhNghiem                                                    #Kinh Nghiệm
        #************************************************************
        for i in range(len(script)):
            if "Your role & responsibilities" in script[i]:
                script_after = script[i].split("Your role & responsibilities")[1]
                break
        if "Your skills & qualifications" in script_after:
            script_after_2 = script_after.split("Your skills & qualifications")[0]
        soup = BeautifulSoup(script_after_2, 'html.parser')
        Mota = soup.get_text(separator='\n', strip=True)
        item['MoTa'] = decode_special_string(Mota)                                          #Mô tả
        #****************************************************************
        yield item
            