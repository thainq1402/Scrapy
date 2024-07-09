import scrapy
from DevWork.items import DevWorkItem
import random
from time import sleep
from DevWork.pipelines import DatabaseConnector
from datetime import date


class DevworkSpider(scrapy.Spider):
    # Name spider
    name = "devwork"
    allowed_domains = ["devwork.vn"]
    start_urls = ["https://devwork.vn/viec-lam?page=1"]
    db_connector = DatabaseConnector(host='103.56.158.31', port = 3306, user='tuyendungUser', password='sinhvienBK', database='ThongTinTuyenDung')
    remove_url_list_local = db_connector.get_links_from_database()
    remove_url_list = remove_url_list_local
    print("Số lượng url trong CSDL: ", len(remove_url_list))
    
    def parse(self, response):
        job_url_list = []
        job_list = response.css('div[class="listing"]')
        # Lấy các link tin tại 1 trang 
        for job_num in range(len(job_list)):
            job_url_list.append(job_list[job_num].css('a::attr(href)').get())
        # Kiểm tra đường dẫn có domain
        for job_url in job_url_list:
            if "https://devwork.vn" in job_url:
                job_next_url = job_url
            else:
                job_next_url = "https://devwork.vn" + job_url
            # Check xem đã có link trong db 
            if job_next_url in self.remove_url_list:
                print("Trùng lặp: ", job_next_url)
                continue
            else:
                yield scrapy.Request(job_next_url, callback=self.parse_job)
        #Trang tiếp theo
        next_page_url = response.css('li[class="pagination-item"] a.page-next::attr(href)').get()
        if next_page_url is not None:
            yield response.follow(next_page_url, callback = self.parse)
    
    def parse_job(self, response):
        ID = "IT_DW_" + str((response.url).split("/")[-2])
        Web = "DevWork"
        Link = response.url
        Nganh = 'IT'
        TenCV = response.css('div[class="header-details"] h1[class="mb-3"]::text').get().replace("\n", "").strip()
        CongTy = response.css('div[class="header-details"] h5[class="mb-10 fw-400"] a::text').get().replace("\n", "").strip()
        TinhThanh = response.css('div[class="header-details"] p::text').get().replace("\n", "").strip()
        Luong = response.css('div.mt-12 div.salary-amount.mb-4 ::text').get().replace("\n", "").strip()

        # Lấy dữ liẹu ở bảng thông tin 
        #************************************************************************************************
        LoaiHinh = KinhNghiem = CapBac = SoLuong = ""
        col = response.css('div[class="job-overview mt-20"] li')
        for i in range(len(col)):
            if 'Hình thức' in col[i].css('div strong::text').get():
                LoaiHinh = col[i].css('div span::text').get()
            if 'Kinh nghiệm' in col[i].css('div strong::text').get():
                KinhNghiem = col[i].css('div span::text').get()
            if 'Vị trí' in col[i].css('div strong::text').get():
                CapBac = col[i].css('div span::text').get()
            if 'Hạn nộp hồ sơ' in col[i].css('div strong::text').get():
                HanNopCV = col[i].css('div span::text').get()
            else:
                HanNopCV = date.today()
            if 'Số lượng' in col[i].css('div strong::text').get():
                SoLuong = col[i].css('div span::text').get()
        #****************************************************************
        # Lấy trường mô tả 
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
        
