# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class VnWorkItem(scrapy.Item):
    ID = scrapy.Field()
    Web = scrapy.Field()
    Nganh = scrapy.Field()
    Link = scrapy.Field()
    TenCV = scrapy.Field()
    CongTy = scrapy.Field()
    TinhThanh = scrapy.Field()
    Luong = scrapy.Field()
    LoaiHinh = scrapy.Field()
    KinhNghiem = scrapy.Field()
    CapBac = scrapy.Field()
    HanNopCV = scrapy.Field()
    YeuCau = scrapy.Field()
    MoTa = scrapy.Field()
    PhucLoi = scrapy.Field()
    SoLuong = scrapy.Field()
