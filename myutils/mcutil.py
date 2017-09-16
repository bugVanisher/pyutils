#!/usr/bin/env python
# coding:utf-8

"""
 Usage:
                my_program <host> <port> -l
                my_program <host> <port> -g <key>
                my_program <host> <port> -d <key>
                my_program <host> <port> -f
                my_program <host> <port> -t <nn>
                my_program (-h | --help )

 Options:
                -h, --help  Show this screen and exit.
                -l  list all keys
                -g <key>  get key value
                -d <key>  delete key
                -f   flush_all
                -t <nn>  top nn largest key-value
"""
import argparse
import logging
import re
import socket
import sys
import time

try:
    from docopt import docopt
except:
    pass


class MemcacheServer(object):
    def __init__(self, host, port):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(host, port)

    def connect(self, host=None, port=None):
        try:
            self.server.connect((host, port))
        except:
            print("connect error ......")

    def send(self, msg):
        self.server.send(msg)

    def get_msg(self):
        buf_len = 1024
        msg = ""
        while True:
            buf = self.server.recv(buf_len)
            msg += buf
            if len(buf) != buf_len:
                break
        return msg

    def get_dump_msg(self):
        """
            仅限特定命令使用
        :return: str
        """
        buf_len = 1024
        msg = ""
        while True:
            buf = self.server.recv(buf_len)
            msg += buf
            if len(buf) != buf_len and msg.endswith("END\r\n"):  # 结束标志
                break
        return msg

    def close(self):
        if self.server:
            self.server.close()


class MCOperation(MemcacheServer):
    """
        缓存操作
    """
    outputList = []

    def __init__(self, host, port):
        MemcacheServer.__init__(self, host, port)
        self.totalItems = 0
        self.allKeysDict = {}
        # self._init_allkeys_val()

    def _init_all_keys_val(self):
        try:
            self.send("stats items\r\n")
            stats_items = self.get_dump_msg()
            # print stats_items
            items = {}
            for line in stats_items.splitlines():
                match = re.search(u"^STAT items:(\d*):number (\d*)", line)
                if match:
                    # print line
                    i, j = match.groups()
                    items[int(i)] = int(j)
                    self.totalItems += int(j)
            for buckets in sorted(items.keys()):
                self.send("stats cachedump %d %d\r\n" % (buckets, 0))
                cachedump = self.get_dump_msg()

                for line in cachedump.splitlines():
                    self._info_filter(line)

        except Exception as e:
            logging.exception("init all keys error %s", e)

    def _format_data(self, record):
        if isinstance(record, dict):
            return "key: \033[31m%s\033[0m [size:\033[32m%s\033[0m expire:\033[32m%s\033[0m]" % (
                record["key"], record["size"], record["expire"])

    def _info_filter(self, line):
        prog = re.compile("ITEM\s(.*?)\s\[(\d{0,20})\sb;\s(\d{10,15})\ss\]")
        match = prog.search(line)
        if match:
            key, size, expire = match.groups()
            # size = "%.2f KB" % (int(size) / 1024)
            expire = time.strftime("%Y年%m月%d日%H:%M:%S", time.gmtime(int(expire) + 60 * 60 * 8))  # 时区差8小时
            if not self.allKeysDict.get(key):
                self.allKeysDict[key] = {"size": size, "expire": expire, "key": key}

    def get_key(self, key):
        self._init_all_keys_val()
        # print self.allKeysDict
        if self.allKeysDict.get(key):
            keydetail = self._format_data(self.allKeysDict.get(key))
            self.send("get %s\r\n" % key)
            content = self.get_msg()
            self.outputList.append(keydetail + "\n" + content)
            return self.outputList
        else:
            return ["key not exist"]

    def get_all_keys(self):
        self._init_all_keys_val()
        for key, val in dict.iteritems(self.allKeysDict):
            keydetail = self._format_data(val)
            self.outputList.append(keydetail)
        return self.outputList

    def del_key(self, key):
        self.send("delete %s\r\n" % key)
        return [self.get_msg()]

    def flush_all(self):
        self.send("flush_all\r\n")
        return [self.get_msg()]

    def get_largest_keys(self, top):
        self._init_all_keys_val()
        size2keysdict = {}
        for k, v in dict.iteritems(self.allKeysDict):
            try:
                samesizekeys = []
                for key in self.allKeysDict.keys():
                    if self.allKeysDict[key]["size"] == v["size"]:
                        samesizekeys.append(key)
                size2keysdict[v["size"]] = samesizekeys
            except Exception as e:
                logging.error("key size error %s:%s", v, e)
        sizes = size2keysdict.keys()
        for i in range(len(sizes)):
            sizes[i] = int(sizes[i])
        sizes = sorted(sizes, reverse=True)
        if len(size2keysdict.keys()) <= top:
            for size in sizes:
                for key in size2keysdict[size]:
                    self.outputList.append(self._format_data(self.allKeysDict[key]))
            return self.outputList
        for i in range(top):
            for key in size2keysdict[str(sizes[i])]:
                self.outputList.append(self._format_data(self.allKeysDict[key]))
        return self.outputList

    def set(self, key, value, expire=0):
        """

        :typ key: str
        :type value: str | bytearray
        :type expire: int
        :return:
        """
        _set = "set {key} 0 {expire} {size}\r\n{value}\r\n".format(key=key, expire=expire, size=len(value), value=value)
        self.send(_set)
        return self.get_msg()


def arg_parse():
    parser = argparse.ArgumentParser(description="memcache command line util.")
    parser.add_argument('host')
    parser.add_argument('port', type=int)
    parser.add_argument('list', help="list all keys", nargs="?")
    parser.add_argument('-g', help="get key value")
    parser.add_argument('-d', help="delete key")
    parser.add_argument('-f', help="flush_all")
    parser.add_argument('-t', help="top largest key-value", type=int)
    result = parser.parse_args(args=sys.argv[1:])
    return vars(result)


if __name__ == '__main__':
    try:
        arguments = docopt(__doc__, sys.argv[1:])
        mcClient = MCOperation(arguments["<host>"], int(arguments["<port>"]))
        if arguments["-g"]:
            print("\n".join(mcClient.get_key(arguments["-g"])))
        elif arguments["-d"]:
            print("\n".join(mcClient.del_key(arguments["-d"])))
        elif arguments["-l"]:
            print("\n".join(mcClient.get_all_keys()))
        elif arguments["-f"]:
            print("\n".join(mcClient.flush_all()))
        elif arguments["-t"]:
            print("\n".join(mcClient.get_largest_keys(int(arguments["-t"]))))
        sys.exit()
    except NameError:
        logging.exception("module docopt is not installed~ ,you'd better install it first~")
    except Exception as e:
        logging.exception(e)

    try:
        arguments = arg_parse()
        mcclient = MCOperation(arguments.get("host"), arguments.get("port"))
        if arguments.get("list"):
            print("\n".join(mcclient.get_all_keys()))
        if arguments.get("g"):
            print("\n".join(mcclient.get_key(arguments.get("g"))))
        if arguments.get("d"):
            print("\n".join(mcclient.del_key(arguments.get("d"))))
        if arguments.get("f"):
            print("\n".join(mcclient.flush_all()))
        if arguments.get("t"):
            print("\n".join(mcclient.get_largest_keys(arguments.get("t"))))
    except Exception as e:
        logging.exception(e)
