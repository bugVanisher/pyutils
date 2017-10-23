#!/usr/bin/env python
# coding: utf-8


"""
    Created by heyu on 2017-04-21
"""
import threading
import time
import random
from mysqlpool import *


HOST = ''
USER = ''
DBPASSWORD = ''
PORT = 3307
DB = ""

HOST2 = ''
USER2 = ''
DBPASSWORD2 = ''
PORT2 = 3306
DB2 = ""

try:
    from local_settings import *
except ImportError as e:
    pass

dbconfig2 = DBConfig(host=HOST2, port=PORT2, user=USER2, pwd=DBPASSWORD2, db=DB2)
dbconfig = DBConfig(host=HOST, port=PORT, user=USER, pwd=DBPASSWORD, db=DB)

class OneQuery(threading.Thread):
    def __init__(self, dbconfig, pool):
        '''
        
        :type dbconfig: 
        :type pool: MysqlPool
        '''
        super(OneQuery, self).__init__()
        self.dbconfig = dbconfig
        self.pool = pool

    def run(self):
        time.sleep(random.randint(1, 10) * 0.1)
        conn = self.pool.get_resource(self.dbconfig)
        if conn:
            print(conn.query("SELECT id FROM `settings`;"))
        self.pool.return_resource(conn)


threads = []
mpool = MysqlPool.get_instance()
mpool.init_pool(dbconfig, 5)
configs = (dbconfig, dbconfig2)
for i in range(20):
    t = OneQuery(dbconfig, mpool)
    t.start()
    threads.append(t)

for th in threads:
    th.join()
