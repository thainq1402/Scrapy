import scrapy
import math
import numpy as np
import json
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from Career.pipelines import DatabaseConnector
from Career.items import CBItem
from datetime import date

class CareerSpider(scrapy.Spider):
    name = "career"
    allowed_domains = ["careerviet.vn"]
    
    def start_requests(self):
        db_connector = DatabaseConnector(host='103.56.158.31', port = 3306, user='tuyendungUser', password='sinhvienBK', database='ThongTinTuyenDung')
        remove_url_list_local = db_connector.get_links_from_database()
        self.remove_url_list = remove_url_list_local
        print("Số lượng url trong CSDL: ", len(self.remove_url_list))
        yield scrapy.Request("https://careerviet.vn/viec-lam/tat-ca-viec-lam-vi.html", callback = self.parse)
        
    def parse(self, response):
        job_count_text = response.css('div.job-found-amout h1::text').get()
        number_cv = ''.join(filter(str.isdigit, job_count_text))
        cv_count = int(number_cv)
        print("Số lượng công việc lấy được: ", cv_count)
        if cv_count % 50 == 0:
            max_page = int(cv_count / 50)
        else:
            max_page = math.floor(cv_count/50) + 1
        # max_page = 10
        for page_number in range(1, max_page + 1):
            page_url = f"https://careerviet.vn/viec-lam/tat-ca-viec-lam-trang-{page_number}-vi.html"
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
        "Origin": "https://careerviet.vn",
        "Referer": url,
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
        }    
        
        #Gửi dữ liệu và lấy kết quả trả về dạng json
        yield scrapy.Request("https://careerviet.vn/search-jobs", method = 'POST', body=json.dumps(payload), headers = header, callback = self.json_parse, meta={'url_list_1': url_list_1, 'page_number': page_number})
        
        #*****************************************************************************************************
    def json_parse(self, response):
        json_data = response.json()
        page_number = response.meta.get('page_number')
        url_list_1 = response.meta.get('url_list_1', [])
        url_list_2 = []
        for i in range(len(json_data["data"])):
            url_list_2.append(json_data["data"][i]["LINK_JOB"])                            #Lấy được mảng url_list_2  
        url_list = url_list_1 + url_list_2
        print("Số url của trang: ", page_number, "là: ", len(url_list))
        for j in range(len(url_list)):
            if url_list[j] in self.remove_url_list:
                print("Trùng lặp: ", url_list[j])
                continue
            else:
                yield scrapy.Request(url_list[j], callback = self.job_parse)
        
    def job_parse(self, response):
        ID = "CB_" + response.url.split(".")[-2]
        Web = "CareerViet"
        Link = response.url
        Nganh =""
        Luong =""
        LoaiHinh =""
        CapBac =""
        HanNopCV = date.today() #Trường kinh nghiệm đã được xử lí phía dưới
        json_content = response.xpath('//script[@type="application/ld+json"]')[1].extract()
        soup = BeautifulSoup(json_content, "html.parser")
        json_content = soup.find("script", type="application/ld+json").string.replace("\n", "").replace("\t", "").replace("\r", "")
        json_data = json.loads(json_content)
        try:
            Img = json_data['hiringOrganization']['logo']
        except:
            Img = ""
        #************************************************************************************************
        try:
            col_1 = response.css('div[class="detail-box has-background"]')[0]       #Loại 1
            for i in range(len(col_1.css('ul li'))):
                try:
                    if 'Ngành nghề' in col_1.css('ul li')[i].css('strong::text').extract():
                        Nganh = col_1.css('ul li')[i].css('p a::text').getall()[0].split("/")[0].strip()
                    if 'Hình thức' in col_1.css('ul li')[i].css('strong::text').extract():
                        LoaiHinh = col_1.css('ul li')[i].css('p::text').get()
                except:
                    continue
            TenCV = response.css('div.job-desc h1[class="title"]::text').get()
            CongTy = response.css('div.job-desc a[class = "employer job-company-name"]::text').get()
            TinhThanh = response.css('div.map p a::text').get()
            col_2 = response.css('div.detail-box.has-background')[1]
            for i in range(len(col_2.css('ul li'))):
                try:
                    if 'Lương' in col_2.css('ul li')[i].css('strong::text').extract():
                        Luong = col_2.css('ul li')[i].css('p::text').get()
                    if 'Kinh nghiệm' in col_2.css('ul li')[i].css('strong::text').extract():
                        KinhNghiem = col_2.css('ul li')[i].css('p::text').get()
                    if 'Cấp bậc' in col_2.css('ul li')[i].css('strong::text').extract():
                        CapBac = col_2.css('ul li')[i].css('p::text').get()
                    if 'Hết hạn nộp' in col_2.css('ul li')[i].css('strong::text').extract():
                        HanNopCV = col_2.css('ul li')[i].css('p::text').get()
                except:
                    continue
            PhucLoi = response.css('ul.welfare-list li::text').getall()
            MoTa = response.css('div.detail-row.reset-bullet ::text').getall()
            YeuCau = response.css('div[class="detail-row"]')[1].css('::text').getall()
        except IndexError:
            try:
                col_1 = response.css('div.col-lg-6')[0]
                for i in range(len(col_1.css('table tr'))):                     #Loại 2
                    try:
                        if 'Ngành nghề' in col_1.css('table tr')[i].css('td')[0].css('p::text').extract():
                            Nganh = col_1.css('table tr')[i].css('td')[1].css('a::text').extract()[0].split("/")[0].strip()
                        if 'Lương' in col_1.css('table tr')[i].css('td')[0].css('p::text').extract():
                            Luong = col_1.css('table tr')[i].css('td')[1].css('p strong::text').get()
                        if 'Hình thức' in col_1.css('table tr')[i].css('td')[0].css('p::text').extract():
                            LoaiHinh = col_1.css('table tr')[i].css('td')[1].css('p::text').get()
                    except:
                        continue
                col_2 = response.css('div.col-lg-6')[1]
                for i in range(len(col_2.css('table tr'))):
                    try:
                        if 'Cấp bậc' in col_2.css('tbody tr')[i].css('td')[0].css('p::text').extract():
                            CapBac = col_2.css('tbody tr')[i].css('td')[1].css('p::text').get()
                        if 'Hết hạn nộp' in col_2.css('tbody tr')[i].css('td')[0].css('p::text').extract():
                            HanNopCV = col_2.css('tbody tr')[i].css('td')[1].css('p::text').get()
                        if 'Kinh nghiệm' in col_2.css('tbody tr')[i].css('td')[0].css('p::text').extract():
                            KinhNghiem = col_2.css('tbody tr')[i].css('td')[1].css('p::text').get()
                    except:
                        continue
                PhucLoi = response.css('ul[class="welfare-list"] li::text').extract()
                MoTa = response.css('div[class="detail-row"]')[0].css('::text').extract()
                YeuCau = response.css('div[class="detail-row"]')[1].css('::text').getall()
                CongTy = response.css('div[class="caption"] a::text').get()
                TenCV = response.css('div[class="title"] h2::text').get()
                TinhThanh = response.css('p[class="list-workplace"] a::text').get()
            except IndexError:
                col_1 = response.css('div[class="table"] table tbody tr')                   #Loại 3
                for i in range(len(col_1)):
                    try:
                        if 'Ngành nghề' in col_1[i].css('td')[0].css('p::text').extract():
                            Nganh = col_1[i].css('td')[1].css('a::text').getall()[0].split("/")[0].strip()
                        if 'Lương' in col_1[i].css('td')[0].css('p::text').extract():
                            Luong = col_1[i].css('td')[1].css('p strong::text').get()
                        if 'Hình thức' in col_1[i].css('td')[0].css('p::text').extract():
                            LoaiHinh = col_1[i].css('td')[1].css('p::text').get()
                        if 'Kinh nghiệm' in col_1[i].css('td')[0].css('p::text').extract():
                            KinhNghiem = col_1[i].css('td')[1].css('p::text').get()
                        if 'Cấp bậc' in col_1[i].css('td')[0].css('p::text').extract():
                            CapBac = col_1[i].css('td')[1].css('p::text').get()
                        if 'Hết hạn nộp' in col_1[i].css('td')[0].css('p::text').extract():
                            HanNopCV = col_1[i].css('td')[1].css('p::text').get()
                    except:
                        continue
                TenCV = response.css('div[class="caption"] div.title h2::text').get() or response.css('div[class="head-template"] div[class="title"] h2::text').get()
                CongTy = response.css('div[class="caption"] a::text').get() or response.css('.company::text').get()
                TinhThanh = response.css('p[class="list-workplace"] a::text').get()
                PhucLoi = response.css('div[class="detail-row box-welfares"] ul li::text').extract()
                MoTa = response.css('div[class="detail-row"]')[0].css('::text').getall()
                YeuCau = response.css('div[class="detail-row"]')[1].css('::text').getall()
        #************************************************************************************************
        try:
            KinhNghiem = KinhNghiem.replace("\\r\\n", "").strip()
        except:
            KinhNghiem = "Không có"
        PhucLoi_s = ''
        for PL in PhucLoi:
            PhucLoi_s += PL +", "
        YeuCau_s = ''
        for YC in YeuCau:
            YeuCau_s += YC
        MoTa_s = ''
        for MT in MoTa:
            MoTa_s += MT
        SoLuong = "1"
        item = CBItem()

        try:
            item['ID'] = ID
        except Exception as e:
            item['ID'] = ""

        try:
            item['Web'] = Web
        except Exception as e:
            item['Web'] = ""

        try:
            item['Link'] = Link
        except Exception as e:
            item['Link'] = ""

        try:
            item['Nganh'] = Nganh
        except Exception as e:
            item['Nganh'] = ""

        try:
            item['TenCV'] = TenCV
        except Exception as e:
            item['TenCV'] = ""

        try:
            item['CongTy'] = CongTy
        except Exception as e:
            item['CongTy'] = ""

        try:
            item['TinhThanh'] = TinhThanh
        except Exception as e:
            item['TinhThanh'] = ""

        try:
            item['Luong'] = Luong
        except Exception as e:
            item['Luong'] = ""

        try:
            item['LoaiHinh'] = LoaiHinh
        except Exception as e:
            item['LoaiHinh'] = ""

        try:
            item['KinhNghiem'] = KinhNghiem
        except Exception as e:
            item['KinhNghiem'] = ""

        try:
            item['CapBac'] = CapBac
        except Exception as e:
            item['CapBac'] = ""

        try:
            item['HanNopCV'] = HanNopCV
        except Exception as e:
            item['HanNopCV'] = ""

        try:
            item['PhucLoi'] = PhucLoi_s
        except Exception as e:
            item['PhucLoi'] = ""

        try:
            item['MoTa'] = MoTa_s
        except Exception as e:
            item['MoTa'] = ""

        try:
            item['YeuCau'] = YeuCau_s
        except Exception as e:
            item['YeuCau'] = ""

        try:
            item["SoLuong"] = SoLuong
        except Exception as e:
            item["SoLuong"] = ""
        
        try:
            item["Img"] = Img
        except Exception as e:
            item["Img"] = ""
        yield item
