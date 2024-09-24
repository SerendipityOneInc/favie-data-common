import re
import hashlib
from urllib.parse import urlparse
from datetime import datetime,timezone
from collections.abc import Sized
from dateutil import parser
import tldextract

class CommonUtils():
    @staticmethod
    def all_none(*args):
        return all([arg is None for arg in args])    

    @staticmethod
    def all_not_none(*args):
        return all([arg is not None for arg in args])
    
    @staticmethod
    def any_none(*args):
        return any([arg is None for arg in args])
    
    @staticmethod
    def any_not_none(*args):
        return any([arg is not None for arg in args])
        
    @staticmethod
    def host_trip_www(host:str):
        return re.sub(r'^www\.', '', host) if host is not None else None
    
    @staticmethod
    def md5_hash(text:str):
        return hashlib.md5(text.encode()).hexdigest()
    
    @staticmethod
    def list_len(list):
        return len(list) if list is not None else 0
    
    @staticmethod
    def not_empty(collection):
        return not CommonUtils.is_empty(collection)
    
    @staticmethod
    def is_empty(collection):
        """
        判断给定的集合对象是否为空。
        
        支持的类型包括：
        - 内置类型：list, tuple, set, dict, str
        - 自定义的可迭代对象
        - 任何实现了 __len__ 方法的对象
        
        Args:
        collection: 要检查的集合对象

        Returns:
        bool: 如果集合为空返回 True，否则返回 False
        
        Raises:
        TypeError: 如果传入的对象不是支持的类型
        """
        if collection is None:
            return True
        
        if isinstance(collection, Sized):
            return len(collection) == 0
        
        try:
            iterator = iter(collection)
            next(iterator)
            return False
        except StopIteration:
            return True
        except TypeError:
            raise TypeError(f"Object of type {type(collection).__name__} is not iterable or sized")

    
    @staticmethod
    def current_timestamp():
        return datetime.now().timestamp()
    
    def utc_string_to_timestamp(utc_string: str) -> float:
        """
        将 UTC 时间字符串转换为 UNIX 时间戳。
        
        支持的格式:
        - "2024-08-29T10:17:21.164262Z"
        - "2024-08-29T10:17:21.164262+00:00"
        
        :param utc_string: UTC 时间字符串
        :return: UNIX 时间戳（浮点数）
        """
        try:
            # 使用 dateutil 解析时间字符串
            dt = parser.parse(utc_string)
            
            # 确保时间是 UTC
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                dt = dt.replace(tzinfo=timezone.utc)
            elif dt.tzinfo != timezone.utc:
                dt = dt.astimezone(timezone.utc)
            
            # 返回时间戳
            return dt.timestamp()
        except ValueError as e:
            raise ValueError(f"无法解析时间字符串: {utc_string}. 错误: {str(e)}")
    
    @staticmethod
    def divide_chunks(lst, n):
        # 计算每个分片应有的长度
        chunk_size = len(lst) // n + (1 if len(lst) % n > 0 else 0)
        # 生成分片
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
    
    @staticmethod
    def get_hostname(url):
        """获取URL的主机名"""
        parsed_url = urlparse(url)
        return  parsed_url.hostname
    
    @staticmethod
    def get_domain(url):
        """获取URL的域名"""
        ext = tldextract.extract(url)
        domain = ext.domain + '.' + ext.suffix
        return domain
    
    @staticmethod
    def get_sub_domain(url):
        """获取主机名对应的域名"""
        ext = tldextract.extract(url)
        return ext.subdomain   
    
    @staticmethod
    def get_full_subdomain(url):
        return  CommonUtils.get_hostname(url)
    




