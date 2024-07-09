import scrapy
from DevWork.items import DevWorkItem
import random
from time import sleep

class DevworkSpider(scrapy.Spider):
    name = "devwork"
    allowed_domains = ["devwork.vn"]
    start_urls = ["https://devwork.vn/viec-lam?page=1"]
    def parse(self, response):
        job_url_list = []
        job_list = response.css('div[class="listing"]')
        for job_num in range(len(job_list)):
            job_url_list.append(job_list[job_num].css('a::attr(href)').get())
        for job_url in job_url_list:
            if "https://devwork.vn" in job_url:
                job_next_url = job_url
            else:
                job_next_url = "https://devwork.vn" + job_url
            yield scrapy.Request(job_next_url, callback=self.parse_job)
            sleep(random.randint(1, 3))
        next_page_url = response.css('li[class="pagination-item"] a.page-next::attr(href)').get()
        if next_page_url is not None:
            yield response.follow(next_page_url, callback = self.parse)
    
    def parse_job(self, response):
        ID = "IT_DW_" + str((response.url).split("/")[-2])
        Web = "DevWork"
        Link = response.url
        Nganh = 'IT'
        TenCV = response.css('div[class="header-details"] h1[class="mb-3"]::text').get()
        CongTy = response.css('div[class="header-details"] h5[class="mb-10 fw-400"] a::text').get()
        TinhThanh = response.css('div[class="header-details"] p::text').get()
        #************************************************************************************************
        col = response.css('div[class="job-overview mt-20"] li')
        Luong = col[0].css('div span::text').get()
        LoaiHinh = col[5].css('div span::text').get()
        KinhNghiem = col[1].css('div span::text').get()
        CapBac = col[3].css('div span::text').get()
        HanNopCV = col[6].css('div span::text').get()
        SoLuong = col[7].css('div span::text').get()
        #****************************************************************
        text = response.css('div[class="block-desc"]')
        MoTa = ''
        MoTas_TG = text[0].css('::text').extract()
        for MoTa_TG in MoTas_TG:
            MoTa += MoTa_TG
        YeuCau = ''
        YeuCaus_TG = text[1].css('::text').extract()
        for YeuCau_TG in YeuCaus_TG:
            YeuCau += YeuCau_TG
        PhucLoi = ''
        PhucLois_TG = text[3].css('::text').extract()
        for PhucLoi_TG in PhucLois_TG:
            PhucLoi += PhucLoi_TG
        #****************************************************************
        item = DevWorkItem()
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
        yield item
        
