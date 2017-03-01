#!/usr/bin/env python
# coding:utf-8

import logging
import os
import sys


class MyLogger():
    '''
        my logger
        默认打印general.log和console日志
    '''
    FORMAT = "%(asctime)s ~ %(levelname)s ~ %(message)s"
    DATEFMT = "[%Y-%m-%d %H:%M:%S]"
    # 使用默认日志
    is_general_log = True
    # 是否输出到控制台
    is_console = True
    _has_set_log = False
    formatter = logging.Formatter(FORMAT, DATEFMT)
    level = logging.DEBUG

    @classmethod
    def disable_console(cls):
        cls.is_console = False

    @classmethod
    def setup_logger(cls, logname, logfile, level=logging.INFO, is_console=True, console_level=logging.DEBUG):
        '''
            自定义的logger,否则使用root logger
        :param logname:
        :param logfile:
        :param level:
        :param is_console:
        :param console_level:
        :rtype: logging.Logger
        '''
        l = logging.getLogger(logname)
        if len(l.handlers):
            return l
        l.setLevel(level)
        base_path = os.path.dirname(logfile)
        if base_path and not os.path.exists(base_path):
            try:
                os.makedirs(base_path)
            except OSError, e:
                MyLogger.exception(e)
        filehandler = logging.FileHandler(logfile, mode='a')
        filehandler.setFormatter(cls.formatter)
        l.addHandler(filehandler)
        #   控制台输出
        if is_console and cls.is_console:
            cls.set_console_handler(logname, console_level)
        exception_handler = logging.FileHandler(os.path.join(base_path, "error.log"), mode='a')
        exception_handler.setFormatter(cls.formatter)
        exception_handler.setLevel(logging.ERROR)
        l.addHandler(exception_handler)
        return l

    @classmethod
    def get_logger(cls, logname=None):
        '''
            获取logger
        :param logname:
        :rtype: logging.Logger
        '''
        return logging.getLogger(logname)

    @classmethod
    def _fire_log(cls):
        if cls.is_general_log and not cls._has_set_log:
            logging.basicConfig(filename="general.log", level=cls.level, format=cls.FORMAT, datefmt=cls.DATEFMT)
            exception_handler = logging.FileHandler("error.log", mode='a')
            exception_handler.setFormatter(cls.formatter)
            exception_handler.setLevel(logging.ERROR)
            logging.getLogger().addHandler(exception_handler)
            if cls.is_console:
                cls.set_console_handler()
            cls._has_set_log = True

    @classmethod
    def set_console_handler(cls, logname=None, console_level=logging.DEBUG):
        '''
            是否输出到控制台
        :param logname:
        :param console_level:
        :return:
        '''
        l = logging.getLogger(logname)
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(cls.formatter)
        streamhandler.setLevel(console_level)
        l.addHandler(streamhandler)

    @classmethod
    def info(cls, msg, *args, **kwargs):
        cls._fire_log()
        logging.info(msg, *args, **kwargs)

    @classmethod
    def debug(cls, msg, *args, **kwargs):
        cls._fire_log()
        logging.debug(msg, *args, **kwargs)

    @classmethod
    def error(cls, msg, *args, **kwargs):
        cls._fire_log()
        logging.error(msg, *args, **kwargs)

    @classmethod
    def critical(cls, msg, *args, **kwargs):
        cls._fire_log()
        logging.critical(msg, *args, **kwargs)

    @classmethod
    def warning(cls, msg, *args, **kwargs):
        cls._fire_log()
        logging.warning(msg, *args, **kwargs)

    @classmethod
    def exception(cls, e, *args, **kwargs):
        cls._fire_log()
        logging.exception(e, *args, **kwargs)

    @classmethod
    def exception_handler(cls, exc_type, exc_value, exc_traceback):
        '''
           自定义的sys.excepthook, 未捕获的异常按root logger的规则输出
        :param exc_type:
        :param exc_value:
        :param exc_traceback:
        :return:
        '''
        cls.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


# Install exception handler
sys.excepthook = MyLogger.exception_handler
