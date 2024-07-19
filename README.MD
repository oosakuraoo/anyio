

setup.py 所需模块看requirements.txt
```
pip freeze > requirements.txt
```

conf/ 配置文件
```eg:
coin_file_type = "csv"  # 目前只有csv完善
coin_output_dir = "data/binance/" + coin_file_type + "/"
stock_conf_dir = "conf/stock/"
stock_conf_code_file = "conf/stock/code.js"
stock_output_dir = "data/stock/"

# 剩余的根据需求自行补缺
```

src/ 业务代码

main.py flask

task.py 包含所有任务启动

tool.py 检查csv数据


