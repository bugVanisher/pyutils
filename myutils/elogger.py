#!/usr/bin/env python
# coding:utf-8
"""
Usage:
    >> os.environ["base_dir"] = os.path.dirname(os.path.realpath(__file__)) # 获取logger前指定日志目录
    >> logger1 = MyLogger.get_logger()

    >> logger2 = MyLogger.setup_logger(__file__, "run.log", is_console=False) # 不输出到控制台

"""
import logging
import os
import sys


class MyLogger(logging.Logger):
    '''
        my logger
    '''
    FORMAT = "%(asctime)s ~ %(levelname)s ~ %(message)s"
    DATEFMT = "[%Y-%m-%d %H:%M:%S]"
    # 是否输出到控制台
    is_console = True
    formatter = logging.Formatter(FORMAT, DATEFMT)
    level = logging.DEBUG

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
        if os.path.isabs(logfile):
            base_path = os.path.dirname(logfile)
        else:
            base_path = os.path.join(cls._get_base_dir(), "logs")
            logfile = os.path.join(base_path, logfile)
        if base_path and not os.path.exists(base_path):
            try:
                os.makedirs(base_path)
            except OSError, e:
                logging.exception(e)
        filehandler = logging.FileHandler(logfile, mode='a')
        filehandler.setFormatter(cls.formatter)
        l.addHandler(filehandler)
        #   控制台输出
        if is_console and cls.is_console:
            cls._set_console_handler(logname, console_level)
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
        if not logname:
            cls.fire_log()
        return logging.getLogger(logname)

    @classmethod
    def fire_log(cls):
        """

        :return:
        """
        if len(logging.root.handlers) == 0:
            logs_path = os.path.join(cls._get_base_dir(), "logs")
            if not os.path.exists(logs_path):
                os.makedirs(logs_path)
            logging.basicConfig(filename=os.path.join(logs_path, "general.log"), level=cls.level,
                                format=cls.FORMAT, datefmt=cls.DATEFMT)
            exception_handler = logging.FileHandler(os.path.join(logs_path, "error.log"), mode='a')
            exception_handler.setFormatter(cls.formatter)
            exception_handler.setLevel(logging.ERROR)
            logging.root.addHandler(exception_handler)
            if cls.is_console:
                cls._set_console_handler()

    @classmethod
    def _set_console_handler(cls, logname=None, console_level=logging.DEBUG):
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
    def _get_base_dir(cls):
        """
            you can set base log dir, otherwise use cwd
        :return:
        """
        path = os.environ.get("base_dir", os.path.dirname(os.getcwd()))
        return path

    @classmethod
    def exception_handler(cls, exc_type, exc_value, exc_traceback):
        '''
           自定义的sys.excepthook, 未捕获的异常按root logger的规则输出
        :param exc_type:
        :param exc_value:
        :param exc_traceback:
        :return:
        '''
        logging.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

# Install exception handler
sys.excepthook = MyLogger.exception_handler
