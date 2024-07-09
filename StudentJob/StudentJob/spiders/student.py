from typing import Iterable
import scrapy
from scrapy.http import Request
import re
from StudentJob.items import IT_Item
from datetime import date
from StudentJob.pipelines import DatabaseConnector
import numpy as np           #Dùng unique để loại bỏ trùng lặp trong list url
class StudentSpider(scrapy.Spider):
    name = "student"
    allowed_domains = ["studentjob.vn"]
        
    def start_requests(self):
        db_connector = DatabaseConnector(host='127.0.0.1', port = 3306, user='root', password='Camtruykich123', database='tuyendung_2')
        # db_connector = DatabaseConnector(host='103.56.158.31', port = 3306, user='tuyendungUser', password='sinhvienBK', database='ThongTinTuyenDung')
        remove_url_list_local = db_connector.get_links_from_database()
        self.remove_url_list = remove_url_list_local
        print("Số lượng url trong CSDL: ", len(self.remove_url_list))
        yield scrapy.Request("https://studentjob.vn/viec-lam", callback = self.parse)
        
    def parse(self, response):
        job_count = response.css('.count-job span::text').get()
        
        so = re.search(r'\b\d+\b', job_count).group()
        if int(so) % 18 == 0:
            max_page = int(so) / 18
        else:
            max_page = int(so) // 18 + 1
        
        for page_number in range(1, int(max_page)+1):
            yield scrapy.Request(f"https://studentjob.vn/viec-lam?p={page_number}", callback = self.it_parse)
                
    
    def it_parse(self, response):
        job_list_urls = response.css('.job-tittle.job-tittle2 a[target="_blank"]').css('::attr(href)').extract()
        for job_url in job_list_urls:
            if 'https://studentjob.vn' in job_url:
                next_url = job_url
            else:
                next_url = 'https://studentjob.vn' + job_url
            
            if next_url in self.remove_url_list:
                print("Trùng lặp: ", next_url)
                continue
            else:
                yield scrapy.Request(next_url, callback = self.it_parse_2)
    
    def it_parse_2(self, response):
        Web = 'StudentJob'
        Link = response.url
        TenCV = response.css('.job-title::text').get().replace("\\r\\n", "").strip()
        CongTy = response.css('.company-name::text').get().replace("\\r\\n", "").strip()
        TinhThanh = response.css('.company-address::text').get().replace("\\r\\n", "").strip().split(",")[-1].split("-")[-1].split("–")[-1].split("_")[-1].strip()
        Luong = response.css('.salary p::text').get()
        for i in range(len(response.css('.summary-content'))):
            if 'Loại công việc' in response.css('.summary-content')[i].css('.content-label::text').get():
                LoaiHinh = response.css('.summary-content')[i].css('.content::text').get()
            if 'Ngành Nghề' in response.css('.summary-content')[i].css('.content-label::text').get():
                Nganh = response.css('.summary-content')[i].css('.content a::text').get()
            if 'Vị trí' in response.css('.summary-content')[i].css('.content-label::text').get():
                try:
                    CapBac = response.css('.summary-content')[i].css('.content::text').get()
                except:
                    CapBac = "Không có"
        KinhNghiem = "Không có"
        MoTa = ""
        MoTa_List = response.css('.job-description *::text').getall()
        for i in range(len(MoTa_List)):
            MoTa += MoTa_List[i]

        YeuCau = ""
        YeuCau_List = response.css('.job-experience  *::text').getall()
        for i in range(len(YeuCau_List)):
            YeuCau += YeuCau_List[i]
        
        PhucLoi = ""
        PhucLoi_List = response.css('.job-benefits  *::text').getall()
        for i in range(len(PhucLoi_List)):
            PhucLoi+=PhucLoi_List[i]
            
        HanNopCV = response.css('div[class="d-flex expiry"] div')[0].css('::text').get().split(":")[-1].strip()
        SoLuong = "1"
        if HanNopCV == "":
            HanNopCV = date.today()
        item = IT_Item()
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
        
        yield item