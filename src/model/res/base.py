from json import JSONEncoder


class ObjData:
    def __init__(self, data=None, msg="", code=200):
        self.data = data
        self.msg = msg
        self.code = code
        # self.encoder = JSONEncoder()

    def to_json(self):
        return {"data": self.data, "msg": self.msg, "code": self.code}


class PageData(ObjData):
    def __init__(self, list=None, page=1, count=0):
        self.list = list
        self.page = page
        self.count = count
        # self.encoder = JSONEncoder()
        super().__init__(
            {"list": self.list, "page": self.page, "count": self.count}, "success", 0
        )


class CustomEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjData):
            return obj.to_json()
        return super().default(obj)
