import re
import pandas as pd
import time
import datetime

class BaseCollector:
    """
    BaseCollector
    Prototype for collectors
    """
    def __init__(self, moduleName = ''):
        self.name = moduleName

    def getCollectorName(self):
        return self.name

    def update(self, updateList = None):
        pass


class CollectorException(Exception):
    """
    Prototype for collector exception
    """
    pass

class ConfigError(CollectorException):
    def __init__(self, msg):
        self.msg = msg
        pass
    def __str__(self):
        return self.msg

