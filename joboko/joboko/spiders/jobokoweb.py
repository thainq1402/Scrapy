import scrapy
from joboko.items import IT_Item
from joboko.pipelines import DatabaseConnector
class JobokowebSpider(scrapy.Spider):
    name = "jobokoweb"
    allowed_domains = ["vn.joboko.com"]

    def start_requests(self):
        # db_connector = DatabaseConnector(host='127.0.0.1', port = 3306, user='root', password='Camtruykich123', database='tuyendung_2')
        db_connector = DatabaseConnector(host='103.56.158.31', port = 3306, user='tuyendungUser', password='sinhvienBK', database='ThongTinTuyenDung')
        remove_url_list_local = db_connector.get_links_from_database()
        self.remove_url_list = remove_url_list_local
        print("Số lượng url trong CSDL: ", len(self.remove_url_list))
        yield scrapy.Request("https://vn.joboko.com/viec-lam-theo-nganh-nghe", callback = self.parse)
    
    def parse(self, response):
        list_branch_url = response.css('div[class="item"] ul li a::attr(href)').extract()
        list_branch_name = response.css('div[class="item"] ul li a span::text').extract()
        for i in range(len(list_branch_url)):
            if 'https://vn.joboko.com' in list_branch_url[i]:
                branch_url = list_branch_url[i]
            else:
                branch_url = 'https://vn.joboko.com' + list_branch_url[i]
            branch_name = list_branch_name[i]
            
            for page_number in range(1, 151):
                branch_page = f"{branch_url}?p={page_number}"  
                yield scrapy.Request(branch_page, callback = self.branch_parse, meta = {'branch_name': branch_name})
                
    def branch_parse(self, response):
        job_url_list = response.css('.item-title a::attr(href)').extract()
        branch_name = response.meta.get("branch_name")
        for job_url in job_url_list:
            if 'https://vn.joboko.com' in job_url:
                next = job_url
            else:
                next = 'https://vn.joboko.com' + job_url
            
            if next in self.remove_url_list:
                print("Trùng lặp: ", next)
                continue
            else:
                yield scrapy.Request(next, callback = self.it_parse, meta = {'branch_name': branch_name})
                
    def it_parse(self, response):
        Web = 'Joboko'
        Nganh = response.meta.get("branch_name")
        Link = response.url
        TenCV = response.css('[class="nw-company-hero__info"] .nw-company-hero__title a::text').get()
        CongTy = response.css('[class="nw-company-hero__info"]  a.nw-company-hero__text::text').get()
        TinhThanh = response.css('[class="nw-company-hero__address"]  a::text').get()
        Luong = response.css('[class="col-12"]  span::text').get()
        KinhNghiem = "Không có"
        CapBac = "Không có"
        for i in range(len(response.css('[class="col-12 col-md-6"]'))):
            if 'Loại hình' in response.css('[class="col-12 col-md-6"]')[i].css('.item-content::text').get():
                LoaiHinh =  response.css('[class="col-12 col-md-6"]')[i].css('span::text').get()
            if 'Kinh nghiệm' in response.css('[class="col-12 col-md-6"]')[i].css('.item-content::text').get():
                KinhNghiem = response.css('[class="col-12 col-md-6"]')[i].css('span::text').get()
            if 'Chức vụ' in response.css('[class="col-12 col-md-6"]')[i].css('.item-content::text').get():
                CapBac = response.css('[class="col-12 col-md-6"]')[i].css('span::text').get()
        
        YeuCau = ""
        MoTa = ""
        PhucLoi =""
        if 'Mô tả công việc' in response.css('[class="fz-15 block-text c-text-2"] h3::text').extract():
            MoTa_List = response.css('.text-left')[0].css('*:not(:empty)::text').getall()
            for i in range(len(MoTa_List)):
                MoTa += MoTa_List[i]
        if 'Yêu cầu công việc' in response.css('[class="fz-15 block-text c-text-2"] h3::text').extract():
            YeuCau_List = response.css('.text-left')[1].css('*:not(:empty)::text').getall()
            for i in range(len(YeuCau_List)):
                YeuCau += YeuCau_List[i]
        if 'Quyền lợi được hưởng' in response.css('[class="fz-15 block-text c-text-2"] h3::text').extract():
            PhucLoi_List = response.css('.text-left')[2].css('*:not(:empty)::text').getall()
            for i in range(len(PhucLoi_List)):
                PhucLoi += PhucLoi_List[i]
        
        HanNopCV = response.css('[class="item-date"]::attr(data-value)').get().split("T")[0]
        SoLuong= "1"

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