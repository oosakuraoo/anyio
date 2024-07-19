import numpy as np
from natsort import natsorted
from src.lib.technical.tool import Tool
from src.service.binance.symbols_klines import SymbolsKlines
from src.lib.technical.slg import CustomSlgOne, CustomSlgTwo
from src.kdata.binance.enums import TIME_FRAME
from src.lib.redis.db import RedisClient
from datetime import datetime, timedelta, timezone

# print(242.657 + 9.437 + 47.905)

import matplotlib.pyplot as plt
from matplotlib.widgets import Slider

import tkinter as tk

root = tk.Tk()
root.title("Tkinter Scrollbar Example")

# 创建一个Listbox和一个垂直滚动条
listbox_scroll = tk.Scrollbar(root)
listbox = tk.Listbox(root, yscrollcommand=listbox_scroll.set)

# 将Listbox和滚动条关联起来
listbox_scroll.config(command=listbox.xview)

# 填充Listbox
for i in range(20):
    listbox.insert(f"Item {i}", tk.END)

# 布局组件
listbox.pack(side=tk.TOP, fill=tk.BOTH)
listbox_scroll.pack(side=tk.BOTTOM, fill=tk.X)

root.mainloop()
