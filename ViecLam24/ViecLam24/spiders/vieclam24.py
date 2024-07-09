import scrapy
import math
from ViecLam24.items import ViecLam24Item
from ViecLam24.pipelines import DatabaseConnector

class Vieclam24Spider(scrapy.Spider):
    name = "vieclam24"
    allowed_domains = ["vieclam24h.vn"]
    
    def start_requests(self):
        # db_connector = DatabaseConnector(host='127.0.0.1', port = 3306, user='root', password='Camtruykich123', database='tuyendung_2')
        db_connector = DatabaseConnector(host='103.56.158.31', port = 3306, user='tuyendungUser', password='sinhvienBK', database='ThongTinTuyenDung')
        remove_url_list_local = db_connector.get_links_from_database()
        self.remove_url_list = remove_url_list_local
        print("Số lượng url trong CSDL: ", len(self.remove_url_list))
        url_get_job = "https://vieclam24h.vn/tim-kiem-viec-lam-nhanh?page=1"
        yield scrapy.Request(url_get_job, callback = self.parse)
        
    def parse(self, response):
        num_job = response.css('div[class="flex items-center"] span[class="font-semibold"]::text').get()
        num_job = num_job.replace(",", "")
        num_job = int(num_job)
        if num_job % 30 == 0:
            num_page = num_job/30
        else:
            num_page = math.floor(num_job/30) + 1
        for page_number in range(1, int(num_page) +1):
        # for page_number in range(1, 200):
            url_page = f"https://vieclam24h.vn/tim-kiem-viec-lam-nhanh?page={page_number}"
            yield scrapy.Request(url_page, callback = self.get_job_list)
    
    def get_job_list(self, response):
        job_list_url = response.css('div.relative a[class="relative lg:h-[115px] w-full flex rounded-sm border lg:mb-3 mb-2 lg:hover:shadow-md !hover:bg-white !bg-[#FFF5E7] border-se-blue-10"]::attr(href)').extract()
        for url_job in job_list_url:
            if "https://vieclam24h.vn"in url_job:
                url_job = url_job
            else:
                url_job = "https://vieclam24h.vn" + url_job
            
            if url_job in self.remove_url_list:
                print("Trùng lặp: ", url_job)
                continue
            else:
                yield scrapy.Request(url_job, callback = self.job_parse)
    
    def job_parse(self, response):
        ID = "VL24_"+ (response.url).split("-")[-1].replace(".html", "")
        Web = "Vieclam24h"
        Link = response.url
        Nganhs_TG = response.css('a.jsx-d84db6a84feb175e::text').extract()
        Nganh = ''
        for Nganh_TG in Nganhs_TG:
            Nganh += Nganh_TG + ","
        TenCV = response.css('h1.leading-snug::text').get()
        CongTy = response.css('div[class="md:ml-7 w-full"] a h3.mb-4::text').get()
        TinhThanh = response.css('div[class="md:ml-7 w-full"] div.flex.items-start a span::text').get()
        Luong = response.css('div[class="md:ml-7 w-full"] div.mt-5 div.ml-3')[0].css('p')[1].css('::text').get()
        HanNopCV = response.css('div[class="md:ml-7 w-full"] div.mt-5 div.ml-3')[1].css('p')[1].css('::text').get()
        #****************************************************************
        col_1 = response.css('div[class="jsx-d84db6a84feb175e md:flex md:border-b border-[#DDD6FE] mb-4"]')
        for i in range(len(col_1[0].css('div.ml-3'))):
            if col_1[0].css('div.ml-3')[i].css('p')[0].css('::text').get() == "Cấp bậc":
                CapBac = col_1[0].css('div.ml-3')[i].css('p')[1].css('::text').get()
        for i in range(len(col_1[1].css('div.ml-3'))):
            if col_1[1].css('div.ml-3')[i].css('p')[0].css('::text').get() == "Số lượng tuyển":
                SoLuong = col_1[1].css('div.ml-3')[i].css('p')[1].css('::text').get()
            if col_1[1].css('div.ml-3')[i].css('p')[0].css('::text').get() == "Hình thức làm việc":
                LoaiHinh = col_1[1].css('div.ml-3')[i].css('p')[1].css('::text').get()
        for i in range(len(col_1[2].css('div.ml-3'))):
            if col_1[2].css('div.ml-3')[i].css('p')[0].css('::text').get() == "Yêu cầu kinh nghiệm":
                KinhNghiem = col_1[2].css('div.ml-3')[i].css('p')[1].css('::text').get()
        MoTa = ''
        MoTas_TG = response.css('div[class="jsx-d84db6a84feb175e"]')[0].css('*:not(:empty)::text').getall()
        for MoTa_TG in MoTas_TG:
            MoTa += MoTa_TG
        YeuCau = ''
        YeuCaus_TG = response.css('div[class="jsx-d84db6a84feb175e mb-4 md:mb-8"] *:not(:empty)::text').getall()
        for YeuCau_TG in YeuCaus_TG:
            YeuCau += YeuCau_TG
        PhucLoi = ''
        PhucLois_TG = response.css('div[class="jsx-d84db6a84feb175e"]')[1].css('*:not(:empty)::text').getall()
        for PhucLoi_TG in PhucLois_TG:
            PhucLoi += PhucLoi_TG
        
        item = ViecLam24Item()
        item['ID'] = ID
        item['Web'] = Web
        item['Link'] = Link
        item['Nganh'] = Nganh
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