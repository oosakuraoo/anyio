import logging
from logging.handlers import TimedRotatingFileHandler
import colorlog
from typing import Text
import os.path
import time
import conf.config as config
from enum import Enum


class LogLevel(Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"
    NOTSET = "NOTSET"


class Logger:
    def __init__(
        self,
        level=LogLevel.INFO.value,
        fmt="%(asctime)s - %(filename)s[%(funcName)s][line:%(lineno)d] - %(levelname)s: %(message)s",
    ):
        # logLevel = logging.getLevelName(level)

        # 控制器输出
        logging.basicConfig(
            level=logging.getLevelName(level),
            format="%(asctime)s - %(levelname)s[%(lineno)d]: %(message)s",
        )

        #
        localtime = time.localtime(time.time())
        log_path = (
            os.getcwd()
            + "/"
            + config.logs_output_dir
            + time.strftime("%Y%m%d", localtime)
            + "/"
        )
        rq = time.strftime("%Y%m%d%H", localtime)
        filename = log_path + rq + ".log"
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        #
        self.logger = logging.getLogger(filename)
        # self.logger.setLevel(logLevel)
        screen_output = logging.StreamHandler()
        screen_output.setFormatter(self.log_color())
        if not self.logger.handlers:  # 避免重复加载日志
            fh = TimedRotatingFileHandler(
                filename=filename, when="H", encoding="utf8"
            )  # , backupCount=3
            fh.setLevel(logging.DEBUG)
            # formatter = logging.Formatter(fmt)
            formatter = CustomFormatter(prefix="src", fmt=fmt)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)
            # self.logger.removeHandler(fh)

    @classmethod
    def log_color(cls):
        """设置日志颜色"""
        log_colors_config = {
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red",
        }
        formatter = colorlog.ColoredFormatter(
            "%(log_color)s[%(asctime)s] [%(name)s] [%(levelname)s]: %(message)s",
            log_colors=log_colors_config,
        )
        return formatter


#######################


class CustomFormatter(logging.Formatter):
    def __init__(self, prefix="", *args, **kwargs):
        self.prefix = prefix
        super().__init__(*args, **kwargs)

    def format(self, record):
        record.filename = record.pathname.split(self.prefix)[-1]
        return super().format(record)
