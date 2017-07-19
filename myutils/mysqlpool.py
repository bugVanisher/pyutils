#!/usr/bin/env python
# coding: utf-8


"""
    Created by heyu on 2017-04-21
"""

import hashlib
import logging

import MySQLdb
import threading
from Queue import Queue
from elogger import MyLogger

__all__ = ["MysqlPool", "DBConfig"]

mysql_logger = MyLogger.setup_logger("mysqllog", "mysql.log")


class Record(object):
    """A row"""
    __slots__ = ('_keys', '_values')

    def __init__(self, keys, values):
        self._keys = keys
        self._values = values

        # Ensure that lengths match properly.
        assert len(self._keys) == len(self._values)

    def keys(self):
        """Returns the list of column names from the query."""
        return self._keys

    def values(self):
        """Returns the list of values from the query."""
        return self._values

    def __getitem__(self, key):
        # Support for index-based lookup.
        if isinstance(key, int):
            return self.values()[key]

        # Support for string-based lookup.
        if key in self.keys():
            i = self.keys().index(key)
            return self.values()[i]

        raise KeyError("Record contains no '{}' field.".format(key))

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def get(self, key):
        try:
            return self[key]
        except Exception as e:
            raise AttributeError(e)

    def __repr__(self):
        return "<Record " + ", ".join(
            [":".join((k, str(v))) if v is not None else ":".join((k, "None")) for k, v in
             zip(self.keys(), self.values())]) + ">"


class DBConfig(object):
    def __init__(self, host="", port=3306, user="", pwd="", db=""):
        """
        
        :type host: str
        :type port: int
        :type user: str
        :type pwd: str
        :type db: str
        """
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = db

    def get_hash(self):
        name = ":".join((self.host, str(self.port), self.db, self.user))
        m = hashlib.md5()
        m.update(name)
        return m.hexdigest()

    def __repr__(self):
        return "<{classname} host={host},port={port}>".format(classname=self.__class__.__name__, host=self.host,
                                                              port=self.port)


class MysqlConnection(object):
    """
        数据库连接对象
    """

    def __init__(self, mydbconfig):
        """
        
        :type mydbconfig: DBConfig
        """
        self.dbconfig = mydbconfig
        try:
            self.__conn = MySQLdb.connect(mydbconfig.host, mydbconfig.user, mydbconfig.pwd,
                                          mydbconfig.db, mydbconfig.port)
            self.sword = self.__conn.cursor()
            self.sword.execute("set names utf8;")
        except Exception as e:
            logging.warn("connect mysql server error\n%s", e)

    def close_conn(self):
        if self.__conn:
            self.__conn.close()

    def select_db(self, db):
        """
            switch database
        :param db:
        :return:
        """
        try:
            self.__conn.select_db(db)
        except Exception as e:
            logging.exception(e)

    def query(self, sql):
        """

        :param sql:
        :return:
        """
        # print query
        self.sword.execute(sql)
        self.__conn.commit()
        results = self.sword.fetchall()
        if self.sword.description:
            keys = [item[0] for item in self.sword.description]
            if len(results) > 1:
                return [Record(keys, row) for row in results]
            elif len(results) == 0:
                return []
            return Record(keys, results[0])
        return results

    def insert_or_update_many(self, query, args):
        """
            批量操作
        :param query:
        :param args:
        :return:
        """
        try:
            rows = self.sword.executemany(query, args)
            self.__conn.commit()
            return rows
        except Exception as e:
            logging.warn("insert or update error\n%s", e)


class ConnectionThread(threading.Thread):
    def __init__(self, dbconfig, num_queue, conn_queue):
        '''
            
        :type dbconfig: 
        :type num_queue: Queue
        :type conn_queue: Queue
        '''
        super(ConnectionThread, self).__init__()
        self.queue = num_queue
        self.conn_q = conn_queue
        self.dbconfig = dbconfig

    def run(self):
        while True:
            if self.queue.empty():
                break
            self.queue.get()
            conn = MysqlConnection(self.dbconfig)
            mysql_logger.debug("get one connection from {}".format(self.dbconfig))
            self.conn_q.put(conn)


class MysqlPool:
    """
        连接池,支持多线程
    """
    __instance = None
    __resources = dict()

    def __init__(self):
        if MysqlPool.__instance is not None:
            raise NotImplemented("This is a singleton class.")

    @staticmethod
    def get_instance():
        """
        
        :rtype: MysqlPool
        """
        if MysqlPool.__instance is None:
            MysqlPool.__instance = MysqlPool()

        return MysqlPool.__instance

    def init_pool(self, mydbconfig, pool_num):
        '''
            init connection pool with theads
        :type mydbconfig: 
        :type pool_num: 
        :return: 
        '''
        if self.__resources.get(mydbconfig.get_hash()):
            return
        self.__resources[mydbconfig.get_hash()] = list()
        pool_queue = Queue()
        for i in xrange(pool_num):
            pool_queue.put(i)
        threads = []

        conn_queue = Queue()
        for i in xrange(5):
            th = ConnectionThread(mydbconfig, pool_queue, conn_queue)
            th.start()
            threads.append(th)
        for th in threads:
            th.join()

        while not conn_queue.empty():
            conn = conn_queue.get()
            self.__resources.get(mydbconfig.get_hash()).append(conn)

    def get_resource(self, mydbconfig):
        """
        
        :type mydbconfig: DBConfig
        :rtype: MysqlConnection | None
        """
        if not self.__resources.get(mydbconfig.get_hash()):
            mysql_logger.error("please init pool first")
            return
        if len(self.__resources.get(mydbconfig.get_hash())) > 0:
            resource = self.__resources.get(mydbconfig.get_hash()).pop(0)
            mysql_logger.info("Using existing resource:{}".format(resource))
            return resource
        else:
            mysql_logger.error("no resource.")
            return

    def return_resource(self, resource):
        """
        
        :type resource: MysqlConnection
        :return: 
        """
        if resource and self.__resources.get(resource.dbconfig.get_hash()):
            self.__resources.get(resource.dbconfig.get_hash()).append(resource)
