import scrapy
from ViecOi.items import ViecOi

class ViecoiSpider(scrapy.Spider):
    name = "viecoi"
    allowed_domains = ["viecoi.vn"]
    start_urls = ['https://viecoi.vn/tim-viec/linh-vuc-it-phan-cung-mang-may-tinh-212.html?page=', 'https://viecoi.vn/tim-viec/linh-vuc-it-phan-mem-lap-trinh-211.html?page=']
    def start_requests(self):
        for page_first_url in self.start_urls:
            for page_number in range(1, 100):
                page_url = page_first_url + str(page_number)
                yield scrapy.Request(page_url, callback = self.parse)
    
    def parse(self, response):
        job_list_url = response.css('div[class = "grid-job-title"] .title-jobs-home a::attr(href)').extract()
        for job_url in job_list_url:
            yield response.follow(job_url, callback = self.job_parse)
    
    def job_parse(self, response):
        ID = "V_O_" + response.url.split("-")[-1].replace(".html", "")
        Web = "ViecOi"
        col = response.css('div[class = "col-xs-12  background_white property-margin-detail py_10"]')
        Nganh = col[0].css('ul li')[1].css('div')[1].css('a::text').getall()
        Link = response.url
        CongTy = col[0].css('ul li')[2].css('div')[1].css('a::text').get()
        TenCV = response.css('h1[class = "title-jobs-home title-detail-middle"]::text').get()
        TinhThanh = col[0].css('ul li')[2].css('div')[1].css('a::text').get()
        HanNopCV = col[0].css('ul li')[3].css('div')[1].css('::text').get()
        SoLuong= col[1].css('ul li')[0].css('div')[1].css('::text').get()
        KinhNghiem = col[1].css('ul li')[2].css('div')[1].css('::text').get()
        CapBac = col[1].css('ul li')[4].css('div')[1].css('a::text').get()
        Luong = response.css('div[class="div-salary"]').css('div')[1].css('div')[1].css('::text').extract()
        LoaiHinh = "Không có"
        MoTa = col[2].css('div[id="des_company"] ::text').getall()
        YeuCau = col[5].css('a[class="tag "] ::text').getall()
        PhucLoi = col[3].css('a[class="tag "] ::text').getall()
        
        item = ViecOi()
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
        
        