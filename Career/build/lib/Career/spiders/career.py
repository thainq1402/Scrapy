import scrapy
import math
import numpy as np
import requests
import json
from urllib.parse import urlencode
from Career.items import CBItem

class CareerSpider(scrapy.Spider):
    name = "career"
    allowed_domains = ["careerbuilder.vn"]
    
    def start_requests(self):
        yield scrapy.Request("https://careerbuilder.vn/viec-lam/tat-ca-viec-lam-vi.html", callback = self.parse)
        
    def parse(self, response):
        job_count_text = response.css('div.job-found-amout h1::text').get()
        number_cv = ''.join(filter(str.isdigit, job_count_text))
        cv_count = int(number_cv)
        if cv_count % 50 == 0:
            max_page = int(cv_count / 50)
        else:
            max_page = math.floor(cv_count/50) + 1
        # max_page = 10
        for page_number in range(1, max_page+1):
            page_url = f"https://careerbuilder.vn/viec-lam/tat-ca-viec-lam-trang-{page_number}-vi.html"
            yield scrapy.Request(page_url, callback = self.job_url_parse)
        
    def job_url_parse(self, response):
        url_list_1 = response.css('.job_link::attr(href)').extract()
        url_list_1 = np.unique(url_list_1)
        url_list_1 = list(url_list_1)                               #Lấy mảng url đầu tiên và loại bỏ trùng lặp
        #************************************************************************************************
        url = response.url
        page_number_str = ''.join(c for c in url.split('-')[-2] if c.isdigit())
        page_number = int(page_number_str)
        #************************************************************************************************
        if page_number >=1 and page_number <= 9:  
            data_one = 'a:1:{s:4:"PAGE";s:1:"' + str(page_number) + '";}'
        elif page_number >=10 and page_number <=99:
            data_one = 'a:1:{s:4:"PAGE";s:2:"' + str(page_number) + '";}'
        elif page_number >=100 and page_number <=999:
            data_one = 'a:1:{s:4:"PAGE";s:3:"' + str(page_number) + '";}'
        data_two = 'a:0:{}'
        #*************************************************************************************************
        # Mã hóa dữ liệu
        encoded_data_one = urlencode({'dataOne': data_one})
        encoded_data_two = urlencode({'dataTwo': data_two})

        # Kết hợp dữ liệu
        payload = f"{encoded_data_one}&{encoded_data_two}"
    
        #Định nghĩa header gửi yêu cầu
        header = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://careerbuilder.vn",
        "Referer": url,
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
        }    
        
        #Gửi dữ liệu và lấy kết quả trả về dạng json
        response_js = requests.post("https://careerbuilder.vn/search-jobs", headers=header, data = payload)
        json_data = response_js.json()
        #*****************************************************************************************************
        url_list_2 = []
        for i in range(len(json_data["data"])):
            url_list_2.append(json_data["data"][i]["LINK_JOB"])                            #Lấy được mảng url_list_2                      
        url_list = url_list_1 + url_list_2
        for url_job in url_list:
            yield scrapy.Request(url_job, callback = self.job_parse)
        
    def job_parse(self, response):
        ID = "CB_" + response.url.split(".")[-2]
        Web = "CareerBuilder"
        Link = response.url
        #************************************************************************************************
        try:
            col_1 = response.css('div[class="detail-box has-background"]')[0]       #Loại 1
            Nganh = col_1.css('ul li')[1].css('p a::text').getall()
            TenCV = response.css('div.job-desc h1[class="title"]::text').get()
            CongTy = response.css('div.job-desc a[class = "employer job-company-name"]::text').get()
            TinhThanh = response.css('div.map p a::text').get()
            LoaiHinh = col_1.css('ul li')[2].css('p::text').get()
            col_2 = response.css('div.detail-box.has-background')[1]
            if len(col_2.css('ul li')) == 3:
                Luong = col_2.css('ul li')[0].css('p::text').get()
                CapBac = col_2.css('ul li')[1].css('p::text').get()
                KinhNghiem = "Không có"
                HanNopCV = col_2.css('ul li')[2].css('p::text').get()
            else:
                Luong = col_2.css('ul li')[0].css('p::text').get()
                CapBac = col_2.css('ul li')[2].css('p::text').get()
                KinhNghiem = col_2.css('ul li')[1].css('p::text').get()
                HanNopCV = col_2.css('ul li')[3].css('p::text').get()
            PhucLoi = response.css('ul.welfare-list li::text').getall()
            MoTa = response.css('div.detail-row.reset-bullet p::text').getall()
            YeuCau = response.css('div[class="detail-row"] p::text').getall()
        except IndexError:
            try:
                col_1 = response.css('div.col-lg-6')[0]                             #Loại 2
                Nganh = col_1.css('table tr')[0].css('td a::text').getall()
                TenCV = response.css('div[class="title"] h2::text').get()
                CongTy = response.css('div[class="caption"] a::text').get()
                TinhThanh = response.css('p[class="list-workplace"] a::text').get()
                Luong = col_1.css('table tr')[1].css('td')[1].css('p strong::text').get()
                LoaiHinh = col_1.css('table tr')[2].css('td')[1].css('p::text').get()
                col_2 = response.css('div.col-lg-6')[1]
                KinhNghiem = col_2.css('tbody tr')[2].css('td')[1].css('p::text').get()
                CapBac = col_2.css('tbody tr')[1].css('td')[1].css('p::text').get()
                HanNopCV = col_2.css('tbody tr')[3].css('td')[1].css('p::text').get()
                PhucLoi = response.css('ul[class="welfare-list"] li::text').extract()
                MoTa = response.css('div[class="detail-row"]')[0].css('p::text').extract()
                YeuCau = response.css('div[class="detail-row"]')[1].css('p::text').extract()
            except IndexError:
                col_1 = response.css('div[class="table"] table tbody tr')                   #Loại 3
                Nganh = col_1[0].css('td')[1].css('a::text').getall()
                TenCV = response.css('div[class="caption"] div.title h2::text').get()
                CongTy = response.css('div[class="caption"] a::text').get()
                TinhThanh = response.css('p[class="list-workplace"] a::text').get()
                Luong = col_1[1].css('td')[1].css('p strong::text').get()
                LoaiHinh = col_1[2].css('td')[1].css('p::text').get()
                KinhNghiem = col_1[5].css('td')[1].css('p::text').get()
                CapBac = col_1[4].css('td')[1].css('p::text').get()
                HanNopCV = col_1[6].css('td')[1].css('p::text').get()
                PhucLoi = response.css('div[class="detail-row box-welfares"] ul li::text').extract()
                MoTa = response.css('div[class="detail-row"]')[0].css('p::text').extract()
                YeuCau = response.css('div[class="detail-row"]')[1].css('p::text').extract()
        #************************************************************************************************
        KinhNghiem = (' '.join(KinhNghiem.split())).replace("\\r\\n", "")
        PhucLoi_s = ''
        for PL in PhucLoi:
            PhucLoi_s += PL
        YeuCau_s = ''
        for YC in YeuCau:
            YeuCau_s += YC
        MoTa_s = ''
        for MT in MoTa:
            MoTa_s += MT
        Nganh_s =''
        for N in Nganh:
            Nganh_s += N.replace("\\r\\n", "").strip() + ", "
        SoLuong = "1"
        item = CBItem()
        item['ID'] = ID
        item['Web'] = Web
        item['Link'] = Link
        item['Nganh'] = Nganh_s
        item['TenCV'] = TenCV
        item['CongTy'] = CongTy
        item['TinhThanh'] = TinhThanh
        item['Luong'] = Luong
        item['LoaiHinh'] = LoaiHinh
        item['KinhNghiem'] = KinhNghiem
        item['CapBac'] = CapBac
        item['HanNopCV'] = HanNopCV
        item['PhucLoi'] = PhucLoi_s
        item['MoTa'] = MoTa_s
        item['YeuCau'] = YeuCau_s
        item["SoLuong"] = SoLuong
        yield item
                    
                    