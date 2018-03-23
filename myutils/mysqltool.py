#!/usr/bin/env python
# coding: utf-8


"""
    Created by heyu on 2017-04-21
"""

import hashlib
import MySQLdb
import logging

__all__ = ["MysqlConnection", "DBConfig"]

mysql_logger = logging.getLogger("mysqllog")


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
        self.sword = None
        try:
            self.__conn = MySQLdb.connect(mydbconfig.host, mydbconfig.user, mydbconfig.pwd,
                                          mydbconfig.db, mydbconfig.port)
            self.sword = self.__conn.cursor()
            self.sword.execute("set names utf8;")
        except Exception as e:
            mysql_logger.warn("connect mysql server error\n%s", e)

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
            mysql_logger.exception(e)

    def query(self, sql_tpl, args=None):
        """
            参数化,更安全
        :param sql_tpl:
        :return:
        """
        if not self.sword:
            mysql_logger.error("connection is not ready~")
            return
        self.sword.execute(sql_tpl, args)
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
        :type query:
        :type args: list|tuple e.g ((1),)
        :return:
        """
        if not self.sword:
            mysql_logger.error("connection is not ready~")
            return
        try:
            rows = self.sword.executemany(query, args)
            self.__conn.commit()
            return rows
        except Exception as e:
            mysql_logger.exception("insert or update error\n%s", e)
