import conf.config as config
import os
import glob
import pandas as pd


class Page:
    def __init__(self, page=1):
        self.page = page
        if self.page < 1:
            self.page = 1

    def getPageAndData(
        self, symbol, interval, old_df=None, begintime=None, endtime=None
    ):
        file_name = config.coin_output_dir + f"{symbol}/{symbol}_{interval}"
        max_count = len(glob.glob(file_name + "_*.csv"))
        file_i = max_count - self.page + 1
        # print("max_count:{} page:{} file_i:{}".format(max_count, page, file_i))
        if file_i < 1:
            return None, 0
        data = None
        try:
            data = pd.read_csv(file_name + f"_{file_i}.csv")
        except Exception as e:
            print(f"文件 {file_name}_{file_i}.csv 解析失败，错误信息：{e}")
        finally:
            if data is None:
                return None, 0
        df = pd.DataFrame(data)
        if max_count != self.page and self.page > 1:
            if old_df != None and not old_df.empty:
                df = pd.concat([df, old_df])
            elif begintime != None and endtime != None:
                index_df = pd.read_csv(file_name + f"_{file_i+1}.csv")
                begintime_index = index_df.index[
                    index_df["open_time"] == begintime
                ].tolist()
                endtime_index = index_df.index[
                    index_df["open_time"] == endtime
                ].tolist()
                df = pd.concat([df, index_df[begintime_index[0] :: endtime_index[0]]])
        return df, max_count
