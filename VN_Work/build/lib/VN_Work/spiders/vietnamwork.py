import scrapy
import json
import requests
from VN_Work.items import VnWorkItem

class VietnamworkSpider(scrapy.Spider):
    name = "vietnamwork"

    def start_requests(self):
        url = "https://ms.vietnamworks.com/job-search/v1.0/search"
        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Host": "ms.vietnamworks.com",
            "Origin": "https://www.vietnamworks.com",
            "Referer": "https://www.vietnamworks.com/",
            "X-Source": "Page-Container"
        }
        payload = {
                "userId": 0,
                "query": "",
                "filter": [],
                "ranges": [],
                "order": [],
                "hitsPerPage": 50,
                "retrieveFields": [
                    "address",
                    "benefits",
                    "jobTitle",
                    "salaryMax",
                    "isSalaryVisible",
                    "jobLevelVI",
                    "isShowLogo",
                    "salaryMin",
                    "companyLogo",
                    "userId",
                    "jobLevel",
                    "jobLevelId",
                    "jobId",
                    "companyId",
                    "approvedOn",
                    "isAnonymous",
                    "alias",
                    "expiredOn",
                    "industries",
                    "workingLocations",
                    "services",
                    "companyName",
                    "salary",
                    "onlineOn",
                    "simpleServices",
                    "visibilityDisplay",
                    "isShowLogoInSearch",
                    "priorityOrder",
                    "skills",
                    "profilePublishedSiteMask",
                    "jobDescription",
                    "jobRequirement",
                    "prettySalary",
                    "requiredCoverLetter",
                    "languageSelectedVI",
                    "languageSelected",
                    "languageSelectedId"
                ]
            }
        for u in range(100):
            payload["page"] = u
            yield scrapy.Request(
                url,
                method='POST',
                body=json.dumps(payload),
                headers= headers,
                callback=self.parse
            )
            # json_soure = response.json()
            # for i in range(len(json_soure["data"])):
            #     ID = "VNW_" + str(json_soure["data"][i]["jobId"])
            #     Web = "VietNamWork"
            #     Nganh = json_soure["data"][i]["jobFunctionsV3"]["jobFunctionV3NameVI"]
            #     Link = "https://www.vietnamworks.com/" + json_soure["data"][i]["alias"] + "-" +str(json_soure["data"][i]["jobId"]) + "-jv"
            #     TenCV = json_soure["data"][i]["jobTitle"]
            #     CongTy = json_soure["data"][i]["companyName"]
            #     TinhThanh = json_soure["data"][i]["workingLocations"][0]["cityNameVI"]
            #     Luong =json_soure["data"][i]["prettySalary"]
            #     LoaiHinh = "Không có"
            #     KinhNghiem = "Không có"
            #     CapBac= json_soure["data"][i]["jobLevelVI"]
            #     HanNopCV = json_soure["data"][i]["expiredOn"]
            #     YeuCau = json_soure["data"][i]["jobRequirement"]
            #     MoTa = json_soure["data"][i]["jobDescription"]
            #     PhucLoi = ""
            #     for j in range(len(json_soure["data"][i]["benefits"])):
            #         PhucLoi += json_soure["data"][i]["benefits"][j]["benefitValue"]
                    
            #     item = VnWorkItem()
            #     item["ID"] = ID
            #     item["Web"] = Web
            #     item["Nganh"] = Nganh
            #     item["Link"] = Link      
            #     item["TenCV"] = TenCV
            #     item["CongTy"] = CongTy
            #     item["TinhThanh"] = TinhThanh
            #     item["Luong"] = Luong 
            #     item["LoaiHinh"] = LoaiHinh 
            #     item["KinhNghiem"] = KinhNghiem
            #     item["CapBac"] = CapBac 
            #     item["HanNopCV"] = HanNopCV 
            #     item["YeuCau"] = YeuCau
            #     item["MoTa"] = MoTa
            #     item["PhucLoi"] = PhucLoi 
                # yield item
    def parse(self, response):
        json_soure = response.json()
        for i in range(len(json_soure["data"])):
            ID = "VNW_" + str(json_soure["data"][i]["jobId"])
            Web = "VietNamWork"
            Nganh = json_soure["data"][i]["jobFunctionsV3"]["jobFunctionV3NameVI"]
            Link = "https://www.vietnamworks.com/" + json_soure["data"][i]["alias"] + "-" +str(json_soure["data"][i]["jobId"]) + "-jv"
            TenCV = json_soure["data"][i]["jobTitle"]
            CongTy = json_soure["data"][i]["companyName"]
            TinhThanh = json_soure["data"][i]["workingLocations"][0]["cityNameVI"]
            Luong =json_soure["data"][i]["prettySalary"]
            LoaiHinh = "Không có"
            KinhNghiem = "Không có"
            CapBac= json_soure["data"][i]["jobLevelVI"]
            HanNopCV = json_soure["data"][i]["expiredOn"]
            YeuCau = json_soure["data"][i]["jobRequirement"]
            MoTa = json_soure["data"][i]["jobDescription"]
            PhucLoi = ""
            for j in range(len(json_soure["data"][i]["benefits"])):
                PhucLoi += json_soure["data"][i]["benefits"][j]["benefitValue"]
            SoLuong = "1"
                
            item = VnWorkItem()
            item["ID"] = ID
            item["Web"] = Web
            item["Nganh"] = Nganh
            item["Link"] = Link      
            item["TenCV"] = TenCV
            item["CongTy"] = CongTy
            item["TinhThanh"] = TinhThanh
            item["Luong"] = Luong 
            item["LoaiHinh"] = LoaiHinh 
            item["KinhNghiem"] = KinhNghiem
            item["CapBac"] = CapBac 
            item["HanNopCV"] = HanNopCV 
            item["YeuCau"] = YeuCau
            item["MoTa"] = MoTa
            item["PhucLoi"] = PhucLoi 
            item["SoLuong"] = SoLuong
            yield item
    