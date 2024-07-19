from datetime import datetime
import execjs
import requests
import conf.config as config
import os
import js2py
import pandas as pd

# from loguru import logger


# wechatWin = WechatByWindows()
# wechatWin.send_msg("3333333")

# print(datetime.now().timestamp())
"""
1719494000 2024-06-27 21:13:20

1719493913
2024-06-27 21:11:53

1719494623
2024-06-27 21:23:43

1719495939
2024-06-27 21:45:39

1719499731
2024-06-27 22:48:51
"""

# file_name = os.path.join("conf/stock", "code.js")
# # print(file_name)

# with open(file_name, "r", encoding="utf8") as f:
#     js_con = f.read()
#     # print(js_con)


# ctx = execjs.compile(js_con)
# v = ctx.call("get_cookie")
# # v = execjs.compile(x, cwd="node_modules").call("get_cookie")

# # ctx = js2py.eval_js(js_con)
# # v = ctx.get_cookie()

# print("cookie:{}".format(v))
# cookies = {
#     "v": v,
# }

# headers = {
#     "Connection": "keep-alive",
#     "Pragma": "no-cache",
#     "Cache-Control": "no-cache",
#     "Upgrade-Insecure-Requests": "1",
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
#     "Accept-Language": "zh-CN,zh;q=0.9",
# }

# response = requests.get(
#     "http://x.10jqka.com.cn/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E8%8A%AF%E7%89%87&queryarea=",
#     headers=headers,
#     cookies=cookies,
#     verify=False,
# )
# print(response.text)

output_dir = "data/stock_wencai/"
nowdate = datetime.now().strftime("%Y%m%d")
nos = {"600", "601", "603", "000", "001", "002"}
xlsx_file = os.path.join(output_dir, "{}.xlsx".format(nowdate))
df = pd.read_excel(xlsx_file)
data = df.values.tolist()
print("-----------------------")
list = []
for row in data:
    if row[0][:3] not in nos:
        continue
    list.append(
        {
            "no": row[0],
            "name": row[1],
            #
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[15],
            "volume": row[11],
            "amount": row[12],
            "liutong": str(row[13]),
            #
            "bankuai": row[14],
            #
            "zdf": row[16],  # row[10],
            "lb": row[17],
            "hsl": row[18],
            "shizhi": str(row[19]),
        }
    )
print("--------------------------------------")
_df = pd.DataFrame(list)
csv_file = os.path.join(output_dir, "{}.csv".format(nowdate))
print(_df)
_df.to_csv(csv_file, index=False)
