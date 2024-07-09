import scrapy
from CareerLink.items import CareerLinkItem
from datetime import date, timedelta
from CareerLink.pipelines import DatabaseConnector

class CareerlinkSpider(scrapy.Spider):
    name = "careerlink"
    allowed_domains = ["www.careerlink.vn"]
    
    def start_requests(self):

        db_connector = DatabaseConnector(host='103.56.158.31', port = 3306, user='tuyendungUser', password='sinhvienBK', database='ThongTinTuyenDung')

        remove_url_list_local = db_connector.get_links_from_database()

        self.remove_url_list = remove_url_list_local

        print("Số lượng url trong CSDL: ", len(self.remove_url_list))

        yield scrapy.Request('https://www.careerlink.vn/vieclam/list', callback = self.count_job)
        
    def count_job(self, response):
        # region
        ## lấy tổng số job / 50 jobs mỗi page -> Ra số page cần loop 
        job_count = int(response.css('.jobs-count-number::text').get().replace('\n', ""))
        if job_count % 50 ==0:
            max_page = job_count / 50
        else:
            max_page = job_count // 50 +1
        #endregion
            
        for page_number in range(1, max_page+1):
        # for page_number in range(1, 10):
            page_url = "https://www.careerlink.vn/vieclam/list?page=" + str(page_number)
            yield scrapy.Request(url=page_url, callback=self.job_url_parse)
            
    def job_url_parse(self, response):
        # Lấy link của các jobs của 1 page
        job_url_list = response.css('li.list-group-item a.job-link::attr(href)').extract()

        for job_url in job_url_list:
            # Check đường dẫn bao gồm tên miền
            if "https://www.careerlink.vn" in job_url: 
                job_next_url = job_url 
            else:
                job_next_url = "https://www.careerlink.vn" + job_url
            # Check nếu tin đã được crawl về
            if job_next_url in self.remove_url_list:
                print("Trùng lặp: ", job_next_url)
                continue
            else:
                yield scrapy.Request(url=job_next_url, callback=self.job_parse)
    
    def job_parse(self, response):

        ID = "CareerLink_" + (response.url).split("/")[-1].split("?")[0]

        Img = response.css('img.company-img ::attr(src)').get()
        Web = "CareerLink"
        # 
        for i in range(len(response.css('div[class="col-6 pl-1 pr-3 pl-md-2"]').css('div[class="job-summary-item d-block"]'))):
            if response.css('div[class="col-6 pl-1 pr-3 pl-md-2"]').css('div[class="my-0 summary-label"]')[i].css('::text').get() == "Ngành nghề":
                Nganhs_TG = response.css('div[class="col-6 pl-1 pr-3 pl-md-2"]').css('div[class="job-summary-item d-block"]')[i].css('div')[2].css('*:not(:empty)::text').getall()
        Nganh = ''
        for Nganh_TG in Nganhs_TG:
            if Nganh_TG != '\n':
                Nganh += Nganh_TG
        if 'CNTT' in Nganh:
            Nganh = 'IT'

        Link = response.url
        
        TenCV = response.css('h1[class="job-title mb-0"]::text').get() # Lấy attr text -> tên công việc
        CongTy = response.css('p[class="org-name mb-2"] span::text').get()
        TinhThanh = ''
        TinhThanhs_TG = response.css('div[class="d-flex align-items-start mb-2"] *:not(:empty)::text').getall()
        for TinhThanh_TG in TinhThanhs_TG:
            if TinhThanh_TG != '\n':
                TinhThanh += TinhThanh_TG
        Luong = response.css('div[class="d-flex align-items-center mb-2"]')[0].css('span::text').get()
        KinhNghiem = response.css('div[class="d-flex align-items-center mb-2"]')[1].css('span::text').get()
        deadline = response.css('div[class="d-flex align-items-center mb-2"]')[2].css('b::text').get().split("\n")[1]
        # Nếu không có hạn nộp CV -> Lấy ngày crawl làm hạn nộp 
        try:
            HanNopCV = date.today() + timedelta(days = int(deadline))
        except:
            HanNopCV = date.today()
        # 
        for i in range(len(response.css('div[class="col-6 pr-1 pl-3 pr-md-2"] div[class="job-summary-item d-block"]'))):
            if response.css('div[class="col-6 pr-1 pl-3 pr-md-2"] div[class="job-summary-item d-block"]')[i].css('div[class="my-0 summary-label"]::text').get() == "Cấp bậc":
                CapBac = response.css('div[class="col-6 pr-1 pl-3 pr-md-2"] div[class="job-summary-item d-block"]')[i].css('div')[2].css('::text').get()
            if response.css('div[class="col-6 pr-1 pl-3 pr-md-2"] div[class="job-summary-item d-block"]')[i].css('div[class="my-0 summary-label"]::text').get() == "Loại công việc":
                LoaiHinh = response.css('div[class="col-6 pr-1 pl-3 pr-md-2"] div[class="job-summary-item d-block"]')[i].css('div')[2].css('::text').get()
        SoLuong = 1
        MoTa = ''
        MoTas_TG = response.css('div[id="section-job-description"] *:not(:empty)::text').getall()
        for MoTa_TG in MoTas_TG:
            if MoTa_TG != '\n':
                MoTa += MoTa_TG
        YeuCau = ''
        YeuCaus_TG = response.css('div[id="section-job-skills"] *:not(:empty)::text').getall()
        for YeuCau_TG in YeuCaus_TG:
            if YeuCau_TG != '\n':
                YeuCau += YeuCau_TG
        PhucLoi = ''
        PhucLois_TG = response.css('div[id="section-job-benefits"] *:not(:empty)::text').getall()
        for PhucLoi_TG in PhucLois_TG:
            if PhucLoi_TG != '\n':
                PhucLoi += PhucLoi_TG
        # Tạo item 
        item = CareerLinkItem()
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
        item['Img'] = Img
        yield item
        
