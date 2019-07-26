import redis


class ReadConfig:
    def __init__(self, host="redis-config", port=6379, db=0):
        self.r = redis.StrictRedis(host=host, port=port, db=db)

    def get(self, _key):
        if self.r.type(_key) == b"string":
            return self.convert(self.r.get(_key))
        if self.r.type(_key) == b"hash":
            return self.convert(self.r.hgetall(_key))
        if self.r.type(_key) == b"set":
            return self.r.smembers(_key)

    def get_all(self):
        _r_dict = dict()
        _keys = self.r.keys("*")
        for _key in _keys:
            _r_dict[_key] = self.get(_key)
        return _r_dict

    def print_all(self):
        for k, v in self.get_all().items():
            try:
                print(str(k.decode("utf-8") + " --> " + v.decode("utf-8")))
            except AttributeError:
                print(str(k.decode("utf-8") + " --> " + str(v)))
            except Exception as e:
                print(e)
                print(k)
                print(v)

    def convert(self, data):
        data_type = type(data)
        if data_type == bytes: return data.decode()
        if data_type in (str, int): return str(data)
        if data_type == dict: data = data.items()
        return data_type(map(self.convert, data))


if __name__ == "__main__":
    red = ReadConfig(host="127.0.0.1")
    red.print_all()
