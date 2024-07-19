import socks
import socket
import requests
from io import StringIO
import json


# 贪婪恐慌指数
class AlternativeApi:
    def __init__(self, limit=1):
        # https://api.alternative.me/fng/?limit=10&format=csv
        self.url = f"https://api.alternative.me/fng/?limit={limit}"

    def get(self):
        result = None
        response = requests.get(self.url)
        if response.status_code == 200:
            result = json.loads(response.text).get("data")
            for item in result:
                item["typename"] = self.typename(int(item["value"]))
        else:
            print(f"Failed to retrieve data: Status {response.status_code}")
        return result

    def typename(self, value):
        if value < 20:
            return "极度恐慌"
        elif value < 40 and value >= 20:
            return "恐慌"
        elif value < 60 and value >= 40:
            return "中立"
        elif value < 80 and value >= 60:
            return "贪婪"
        else:
            return "极度贪婪"


# socks.set_default_proxy(socks.HTTP, "127.0.0.1", 8080)
# socket.socket = socks.socksocket


# # client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# # client.connect(('127.0.0.1', 8888))

# sock = socket.socket()
# sock.connect(("api.alternative.me", 80))

# request = "GET /index.html HTTP/1.1\r\nHost: example.com\r\n\r\n"
# sock.send(request.encode())

# # 接收响应
# response = sock.recv(4096)
# print(response.decode())

# # 关闭连接
# sock.close()

# #
