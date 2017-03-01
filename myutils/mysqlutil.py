#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

import MySQLdb
from MySQLdb.connections import Connection

__all__ = ["MysqlUtil", "close_conns"]


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
            [":".join((k, v)) if v is not None else ":".join((k, "None")) for k, v in
             zip(self.keys(), self.values())]) + ">"


class MysqlConnection():
    '''
        数据库连接,单例模式,不支持多线程
    '''
    mysql_conn_dict = {}

    @classmethod
    def get_conn(cls, dbname, ip, username, password, port):
        """

        :type dbname:
        :type ip:
        :type username:
        :type password:
        :type port:
        :rtype: Connection
        """
        key = cls.gen_name(ip, str(port), dbname, username)  # 一个数据库对应一个连接
        if key not in cls.mysql_conn_dict.keys():
            try:
                conn = MySQLdb.connect(ip, username, password, dbname, port)
                if conn:
                    cls.mysql_conn_dict[key] = conn
                    return cls.mysql_conn_dict[key]
            except Exception, e:
                logging.warn("connect mysql server error\n%s", e)
                print("connect mysql server error")
                sys.exit()
        else:
            return cls.mysql_conn_dict[key]

    @classmethod
    def gen_name(cls, *args):
        return ":".join(args)


def close_conns():
    '''
        关闭所有连接
    :return:
    '''
    try:
        for conn in MysqlConnection.mysql_conn_dict.keys():
            MysqlConnection.mysql_conn_dict[conn].close()
    except Exception, e:
        logging.error("mysql conn close error", e)


class MysqlUtil:
    db_pools = {}

    @classmethod
    def get_instance(cls, ip, username, password, db, port):
        '''

        :type ip: str
        :type username: str
        :type password:str
        :type db: str
        :type port: int
        :rtype :MysqlUtil
        '''
        key = MysqlConnection.gen_name(ip, str(port), db, username, password)
        if key in cls.db_pools.keys():
            return cls.db_pools[key]
        cls.db_pools[key] = MysqlUtil(ip, username, password, db, port)
        return cls.db_pools[key]

    def __init__(self, ip, username, password, db, port):
        self.conn = MysqlConnection.get_conn(db, ip, username, password, port)
        self.sword = self.conn.cursor()
        self.sword.execute("set names utf8;")

    def select_db(self, db):
        '''
            switch database
        :param db:
        :return:
        '''
        try:
            self.conn.select_db(db)
        except:
            pass

    def query(self, sql):
        '''

        :param sql:
        :return:
        '''
        # print query
        self.sword.execute(sql)
        self.conn.commit()
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
        '''
            批量操作
        :param query:
        :param args:
        :return:
        '''
        try:
            rows = self.sword.executemany(query, args)
            self.conn.commit()
            return rows
        except Exception, e:
            logging.warn("insert or update error\n%s", e)

    def close(self):
        try:
            self.conn.close()
        except:
            pass
