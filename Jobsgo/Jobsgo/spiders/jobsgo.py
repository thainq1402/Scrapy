import scrapy
import re
from Jobsgo.pipelines import DatabaseConnector
from datetime import date, timedelta
from Jobsgo.items import IT_Item

class JobsgoSpider(scrapy.Spider):
    name = "jobsgo"
    allowed_domains = ["jobsgo.vn"]

    def start_requests(self):
        # db_connector = DatabaseConnector(host='127.0.0.1', port = 3306, user='root', password='Camtruykich123', database='tuyendung_2')
        db_connector = DatabaseConnector(host='103.56.158.31', port = 3306, user='tuyendungUser', password='sinhvienBK', database='ThongTinTuyenDung')
        remove_url_list_local = db_connector.get_links_from_database()
        self.remove_url_list = remove_url_list_local
        print("Số lượng url trong CSDL: ", len(self.remove_url_list))
        yield scrapy.Request("https://jobsgo.vn/viec-lam.html", callback = self.job_count_parse)
    
    def job_count_parse(self, response):
        so_luong_viec_lam_text = re.search(r'\b(\d+)\b', response.css('.mrg-bot-15 h1::text').get()).group()
        if int(so_luong_viec_lam_text) % 50 == 0:
            max_page = int(so_luong_viec_lam_text)/ 50
        else:
            max_page = int(so_luong_viec_lam_text) // 50 + 1
        
        for page_number in range(1, max_page+1):
            yield scrapy.Request(f"https://jobsgo.vn/viec-lam.html?&page={page_number}", callback = self.job_url_parse)
    
    def job_url_parse(self, response):
        job_url_list = response.css('.item-click h3 a[target="_blank"]::attr(href)').extract()
        for job_url in job_url_list:
            if job_url in self.remove_url_list:
                print("Trùng lặp: ", job_url)
                continue
            else:
                yield scrapy.Request(job_url, callback = self.job_parse)
    
    def job_parse(self, response):
        Web = 'Jobsgo'
        for i in range(len(response.css('div[class="content-group"]'))):
            if 'Ngành nghề' in response.css('div[class="content-group"]')[i].css('::text').extract():
                Nganh = response.css('div[class="content-group"]')[i].css('div a::text').get()
            if 'Yêu cầu công việc' in response.css('div[class="content-group"]')[i].css('::text').extract():
                YeuCau_List = response.css('div[class="content-group"]')[i].css('::text').extract()
            if 'Mô tả công việc' in response.css('div[class="content-group"]')[i].css('::text').extract():
                MoTa_List = response.css('div[class="content-group"]')[i].css('::text').extract()
            if 'Quyền lợi được hưởng' in response.css('div[class="content-group"]')[i].css('::text').extract():
                PhucLoi_List = response.css('div[class="content-group"]')[i].css('::text').extract()
        Link = response.url
        TenCV = response.css('div.media-body-2 h1::text').get()
        CongTy = response.css('div[class="panel-body"] div[class="media-body"] h2 a::text').get()
        try:
            TinhThanh = response.css('div[class="data giaphv"] p::text').extract()[0].replace("\n", "").strip().split(",")[-1].split("-")[-1].split("–")[-1].split("_")[-1].strip()
        except:
            TinhThanh = response.css('div[class="data giaphv"]::text').extract()[0].replace("\n", "").strip().split(",")[-1].split("-")[-1].split("–")[-1].split("_")[-1].strip()
        Luong = response.css('.saraly::text').get()
        for i in range(len(response.css('div[class="col-sm-4 col-xs-6"]'))):
            if 'Tính chất công việc' in response.css('div[class="col-sm-4 col-xs-6"]')[i].css('::text').extract():
                try:
                    LoaiHinh = response.css('div[class="col-sm-4 col-xs-6"]')[i].css('p')[1].css('a::text').get().strip()
                except:
                    LoaiHinh = response.css('div[class="col-sm-4 col-xs-6"]')[i].css('p')[1].css('::text').get().strip()
            if 'Yêu cầu kinh nghiệm' in response.css('div[class="col-sm-4 col-xs-6"]')[i].css('::text').extract():
                KinhNghiem = response.css('div[class="col-sm-4 col-xs-6"]')[i].css('p')[1].css('::text').extract()[0].strip()
            if "Vị trí/chức vụ" in response.css('div[class="col-sm-4 col-xs-6"]')[i].css('::text').extract():
                CapBac = response.css('div[class="col-sm-4 col-xs-6"]')[i].css('p')[1].css('::text').extract()[0].strip()
        
        YeuCau =""
        for i in range(len(YeuCau_List)):
            YeuCau += YeuCau_List[i]
        
        MoTa = ""
        for i in range(len(MoTa_List)):
            MoTa += MoTa_List[i]
        
        PhucLoi = ""
        for i in range(len(PhucLoi_List)):
            PhucLoi += PhucLoi_List[i]
        SoLuong = '1'
        
        try:
            deadline = response.css('[class="deadline text-bold text-orange"]::text').get().strip()
            HanNopCV = date.today() + timedelta(days = int(deadline))
        except:
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