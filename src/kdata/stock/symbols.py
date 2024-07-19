import conf.stock.config as other_config
import requests
import execjs
import json
import conf.config as config
from bs4 import BeautifulSoup
from lxml import etree
from selenium import webdriver
import time
import os
import random
import parsel
from src.lib.comm.browser_cookie import BrowserCookie
from urllib.parse import quote
from src.lib.technical.filter import StockFilter

# import tushare as ts
# import baostock as bs
# from jqdatasdk import *

stockFilter = StockFilter()

###############################################


# 同花顺 失败率高
class THSSymbols:
    def __init__(self):
        self.pages = 102
        self.base_url = "https://data.10jqka.com.cn/funds/ggzjl"
        self.page_url = self.base_url + "/field/zdf/order/desc/page/#page#/ajax/1"
        self.fengxian_pages = 11
        self.fengxian_base_url = "https://q.10jqka.com.cn/index/fxjs"
        self.fengxian_page_url = (
            self.fengxian_base_url
            + "/board/fxjsgg/field/zdf/order/desc/page/#page#/ajax/1/"
        )
        self.fengxian_data_file = config.stock_output_dir + "/ths/fengxian_list.json"
        self.v = None
        self.headers = {
            # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept": "text/html, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9",
            # "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Cookie": "",
            # "Hexin-V": "",
            "Host": "data.10jqka.com.cn",
            # "Referer": "https://data.10jqka.com.cn/funds/ggzjl/",
            "User_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            # "X-Requested-With": "XMLHttpRequest",
        }
        self.cookie_keys = {
            "Hm_lvt_f79b64788a4e377c608617fba4c736e2",
            "Hm_lvt_60bad21af9c824a4a0530d5dbf4357ca",
            "Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1",
            "Hm_lpvt_f79b64788a4e377c608617fba4c736e2",
            "Hm_lpvt_60bad21af9c824a4a0530d5dbf4357ca",
            "Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1",
            "v",
        }

        self.headers = {
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }

    def __get_v(self):
        # check TOKEN_SERVER_TIME
        file_name = os.path.join(config.stock_conf_code_file)
        with open(file_name, "r", encoding="utf8") as f:
            js_con = f.read()
        ctx = execjs.compile(js_con)
        v = ctx.call("get_cookie")
        return v

    def __no_price_filter(self, no, price):
        # nos = {"600", "601", "603", "000", "001", "002"}
        # if no[:3] not in nos:
        #     return False
        # if price < self.min_price or price > self.max_price or price == "-":
        #     return False
        return stockFilter.no_price_filter(no, price)

    def __result(self, url):
        response = requests.get(url, headers=self.headers)
        result = []
        if response.status_code == 200:
            pass
        else:
            print(f"Failed to retrieve data: Status {response.status_code}")
        response.close()
        return result

    def __result_with_js(self, url):
        cookies = {
            "v": self.__get_v(),
        }
        response = requests.get(
            url,
            headers=self.headers,
            cookies=cookies,
            verify=False,
        )
        return self.__get_result_with_parsel(response.text)

    def __result_with_browser(self, url):
        browserCookie = BrowserCookie()
        cookies, _ = browserCookie.getCookieFromPath(".10jqka.com.cn")
        cookie_str = ";".join("{}={}".format(k, cookies[k]) for k in self.cookie_keys)
        self.headers["Cookie"] = cookie_str
        #
        chrome_options = webdriver.ChromeOptions()
        for k in self.headers:
            chrome_options.add_argument(f"{k}={self.headers[k]}")
        #
        chrome_options.add_argument("--headless")  # 无头模式
        chrome_options.add_argument("disable-popup-blocking")  # 禁用弹窗
        browser = webdriver.Chrome(options=chrome_options)
        browser.get(url)  # 保证刷新过首页才能正常抓取
        html_page = browser.page_source
        browser.close()
        #
        return self.__get_result_with_etree(html_page)

    def __get_result_with_etree(self, html):
        root = etree.HTML(html)
        no_list = root.xpath("//tr//td[2]/a/text()")
        name_list = root.xpath("//tr//td[3]/a/text()")
        price_list = root.xpath("//tr//td[4]/text()")
        # print(price_list)
        zdf_list = root.xpath("//tr//td[5]/text()")
        hsl_list = root.xpath("//tr//td[6]/text()")

        in_list = root.xpath("//tr//td[7]/text()")
        out_list = root.xpath("//tr//td[8]/text()")
        #
        numlen = len(no_list)
        print("no len: ", numlen)
        list = []
        for i in range(numlen):
            if self.__no_price_filter(no_list[i], float(price_list[i])) == False:
                continue
            list.append(
                {
                    "no": no_list[i],
                    "name": name_list[i],
                    "price": price_list[i],
                    "zdf": zdf_list[i],
                    "hsl": hsl_list[i],
                    "in": in_list[i],
                    "out": out_list[i],
                }
            )
        return list

    def __get_result_with_parsel(self, html):
        selector = parsel.Selector(html)
        trs = selector.css(".m-table tr")[1:]
        trs_len = len(trs)
        print("trs_len: ", trs_len)
        list = []
        for tr in trs:
            symbol = {}
            info = tr.css("td::text").getall()
            info_1 = tr.css("td a::text").getall()
            print(info, info_1)
            if self.__no_price_filter(info[1], float(info[2])) == False:
                continue
            #
            symbol["name"] = info[0]
            symbol["code"] = info[1]
            symbol["price"] = info_1[2]
            symbol["change"] = info_1[3]
            symbol["change_rate"] = info[4]
            # symbol["volume"] = info[5]
            list.append(symbol)
        return list

    #################

    def get_symbols(self):
        return self.get_symbols_with_wencai()
        # fengxian_codes = self.get_fengxian_codes()
        # return
        list = []
        #
        p = 1
        next_page = True
        while next_page:
            url = self.base_url.replace("#page#", str(p))
            result = self.__result_with_js(url)
            numlen = len(result)
            if numlen == 0 or numlen < 50:
                next_page = False
            if result is None:
                next_page = False
                print("requesr error")
                continue

            list.append(result)
            print(f"{p} ------------- end")
            p += 1
            #
            time.sleep(random.randint(5, 15))
        return list

    def get_fengxian_codes(self):
        if os.path.exists(self.fengxian_data_file):
            with open(self.fengxian_data_file, "r") as file:
                data = json.loads(file.read())
                if time.time() - data["ctime"] < 0:
                    return data["data"]
        #
        list = []
        p = 0
        while p <= self.fengxian_pages:
            if p == 0:
                url = self.fengxian_base_url
            else:
                url = self.fengxian_page_url.replace("#page#", str(p))
            p += 1
            #
            cookies = self.getcookiefromchrome()
            self.headers["Cookie"] = self.headers["Cookie"].replace("#v#", cookies["v"])
            # self.headers["Cookie"] = "; ".join(
            #     map(lambda item: f"{item[0]}={item[1]}", cookies.items())
            # )
            print(self.headers["Cookie"])
            self.headers["Hexin-V"] = cookies["v"]
            #
            chrome_options = webdriver.ChromeOptions()
            for k in self.headers:
                chrome_options.add_argument(f"{k}={self.headers[k]}")
            #
            # chrome_options.add_argument("--headless")
            # chrome_options.add_argument("disable-popup-blocking")
            browser = webdriver.Chrome(options=chrome_options)
            browser.get(url)  # 保证刷新过首页才能正常抓取
            html_page = browser.page_source
            # browser.close()
            root = etree.HTML(html_page)
            #
            if p == 0:
                time.sleep(random.randint(5, 10))
                continue
            #
            no_list = root.xpath("//table//td[2]/a/text()")
            print(f"no_list: {no_list} ")
            name_list = root.xpath("//td[3]/a/text()")
            print(f"name_list: {name_list} ")
            zdf_list = root.xpath("//td[4]/text()")
            print(f"zdf_list: {zdf_list} ")
            i = 0
            for item in zdf_list:
                if item < 0:
                    list.append(no_list[i])
                i += 1

            # self.fengxian_pages = root.xpath(
            #     "//div[@id='m=page']//span[@class='page_info']/text()"
            # )

            print(f"fengxian_pages: {self.fengxian_pages} ")
            time.sleep(60)
            # time.sleep(random.randint(10, 40))
            break

        print(f"get_fengxian_codes: {list} {len(list)}")
        # with open(self.fengxian_data_file, "w") as file:
        #     file.write(json.dumps({"data": list, "ctime": time.time() + 86400000}))
        return list

    def get_symbols_with_wencai(self):
        url = (
            "https://www.iwencai.com/unifiedwap/result?w=%E7%8E%B0%E4%BB%B7%3C20%3B%E6%B6%A8%E8%B7%8C%E5%B9%853%25%E5%88%B09%25%3B%E9%87%8F%E6%AF%94%3E1%3B%E6%8D%A2%E6%89%8B%E7%8E%873%25%E5%88%B020%25%3B%E5%B8%82%E5%80%BC%3E15%E4%BA%BF&querytype=stock&addSign="
            + int(time.time())
        )
        "https://www.iwencai.com/gateway/urp/v7/landing/getDataList"
        return self.__result_with_js(url)


###############################################


# 东方财富 推荐 包含各种值比例
class DFCFSymbols:
    def __init__(self):
        self.pages = 105
        self.fields = {
            # "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
            "f2,f3,f4,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18",
            "f100",  # 所属行业
            "f184,f165,f175",  # 主力净占比1、5、10
            "f62",  # 主力净额1
            "f116,f117,f162,f167",  # 总市值、流通市值、市盈、市净
            "f20,f9,f23",  # 总市值、市盈、市净
            "f225",  # 今日主力排名
        }
        self.url_params = {
            # "cb": "jQuery112306424878332371882_1718874172687",  # 个股资金流
            "cb": "jQuery11230922750073964413_1719459987022",
            # jQuery112308363916970621172_1719414710635
            # jQuery11230442262486272043_1719415289878
            # jQuery112303554498681698457_1719415115756
            # jQuery11230442262486272043_1719415289878
            # jQuery112308363916970621172_1719414710635
            # jQuery1123044933372088361745_1719459533029
            "pn": "#page#",
            "pz": 50,  # 每页数量 改变注意调整self.pages
            "po": 1,
            "np": 1,
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "fltt": 2,
            "inv": 2,
            # "dect": 1,
            # "wbp2u": quote("|0|0|0|web"),
            # "fid": "f3",
            "fid": "f62",
            # "fs": quote("m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048"),
            # "fs": quote("m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"),
            "fs": quote(
                "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2"
            ),
            "fields": quote(",".join(self.fields)),
            # "_": int(time.time()),
        }
        self.base_url = f"https://push2.eastmoney.com/api/qt/clist/get?"

    def __get_url(self, page):
        url_param = "&".join(
            "{}={}".format(k, self.url_params[k]) for k in self.url_params
        )
        url = self.base_url + url_param.replace("#page#", str(page))
        # print(url)
        return url

    def __get_type(self, type):
        if type == 1:
            return "SS"  # 沪 SH
        return "SZ"  # 深

    def __get_symbol(self, data):
        # f184,165,175主力净占比1、5、10
        # if data["f184"] == "-" or data["f184"] < 0:
        #     return None, False
        # if data["f184"] == "-" or data["f184"] < 0:
        #     return None, False
        # if data["f165"] == "-" or data["f165"] < 0:
        #     return None, False
        # if data["f175"] == "-" or data["f175"] < 0:
        #     return None, False
        # if data["f225"] > 2000:
        #     return None, False
        if stockFilter.no_price_filter(data["f12"], data["f2"]) == False:
            return None, True
        return {
            "no": data["f12"],
            "type": self.__get_type(data["f13"]),
            "name": data["f14"],
            "price": data["f2"],  # 当前价格
            "zdf": data["f3"],  # 涨跌幅
            "zde": data["f4"],  # 涨跌额
            "hsl": data["f8"],  # 换手率
            "lb": data["f10"],  # 量比
            "sjl": data["f23"],  # 市净值
            "syl": data["f9"],  # 市盈率
            #
            "high": data["f15"],
            "low": data["f16"],
            "open": data["f17"],
            "close": data["f18"],
            #
            "bankuai": data["f100"],
            #
            "zhuli_jzb1": data["f184"],
            "zhuli_jzb5": data["f165"],
            "zhuli_jzb10": data["f175"],
            #
            # "f62",  # 主力净额1
            #
            "shizhi": data["f20"],  # 总市值 f116
            "liutong_shizhi": data["f117"],  # 流通市值
            "shiying": data["f162"],  # 市盈
            "shijing": data["f167"],  # 市净
            #
            "is_true": 0,  # 是否满足筛选条件
        }, True

    def __get_symbol_filter(self, data):
        return stockFilter.get_symbol_filter(data)

    def get_symbols(self):
        list = []
        p = 1
        next_page = True
        while next_page:
            response = requests.get(self.__get_url(p))
            if response.status_code == 200:
                txt = response.text[len(self.url_params["cb"]) + 1 : -2]
                data = json.loads(txt).get("data", [])
                for item in data.get("diff", []):
                    symbol, next_page = self.__get_symbol(item)
                    if symbol is None:
                        continue
                    symbol = self.__get_symbol_filter(symbol)
                    if symbol is None:
                        continue
                    list.append(symbol)
                print(f"{p} ------------- end")
                p += 1
                if next_page == None or p > self.pages:
                    next_page = False
            else:
                print(f"Failed to retrieve data: Status {response.status_code}")
            response.close()
            time.sleep(random.randint(1, 10))
        return list


###############################################


class XinLangSymbols:
    def __init__(self):
        # https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount?node=hs_a
        self.pages = 67
        self.url_params = {
            "page": "#page#",
            "num": 80,
            "sort": "amount",
            "asc": 0,
            "node": "hs_a",
            "symbol": "",
            "_s_r_a": "page",
        }
        self.base_url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?"
        # page=#page#&num=80&sort=amount&asc=0&node=hs_a&symbol=&_s_r_a=page"

    def __get_url(self, page):
        url_param = "&".join(
            "{}={}".format(k, self.url_params[k]) for k in self.url_params
        )
        url = self.base_url + url_param.replace("#page#", str(page))
        # print(url)
        return url

    def __get_symbol(self, data):
        if stockFilter.no_price_filter(data["f1"], float(data["high"])) == False:
            return None
        return {"no": data["f1"], "name": data["f2"], "price": data["f3"]}

    def get_symbols(self):
        list = []
        for i in range(1, self.pages + 1):
            url = self.__get_url(i)
            response = requests.get(url)
            if response.status_code == 200:
                data = json.loads(response.text)
                for item in data:
                    symbol = self.__get_symbol(item)
                    if symbol != None:
                        list.append(symbol)
                print(f"{i} ------------- end")
            else:
                print(f"Failed to retrieve data: Status {response.status_code}")
            response.close()
            time.sleep(random.randint(1, 10))
        return list


###############################################


# class JqSymbols:
#     def __init__(self):
#         self.username = other_config.JOINQUANT_USERNAME
#         self.password = other_config.JOINQUANT_PASSWORD
#         auth(self.username, self.password)
#         # pass

#     def get_symbols(self):
#         # 获取所有股票代码
#         stock_codes = get_all_securities().index.tolist()


# class TsSymbols:
#     def __init__(self):
#         self.pro = ts.pro_api(other_config.TUSHARE_USHARE_TOKEN)
#         self.date = "2024-05-01"
#         if self.lg.error_code == "0":
#             print("login successfully")
#         else:
#             SystemError(f"login failed {self.lg.error_code}, {self.lg.error_msg}")
#         # pass

#     def __del__(self):
#         pass

#     def get_symbols(self):
#         data = self.pro.stock_basic(exchange="", list_status="L", fields="ts_code")
#         stock_codes = data["ts_code"].tolist()
#         for stock_code in stock_codes:
#             print(stock_code)


# class BsSymbols:
#     def __init__(self):
#         self.lg = bs.login()
#         self.date = "2024-05-01"
#         if self.lg.error_code == "0":
#             print("login successfully")
#         else:
#             SystemError(f"login failed {self.lg.error_code}, {self.lg.error_msg}")
#         # pass

#     def __del__(self):
#         bs.logout()

#     def get_symbols(self):
#         stock_rs = bs.query_all_stock(self.date)
#         print(len(stock_rs))
#         stock_df = stock_rs.get_data()
#         data_df = pd.DataFrame(stock_df)
#         stock_codes = data_df.to_numpy().tolist()

#         # stock_codes = []
#         # while stock_rs.error_code == "0" and stock_rs.next():
#         #     stock_codes.append(stock_rs.get_row_data()[0])

#         print(len(stock_codes))
#         return stock_codes
