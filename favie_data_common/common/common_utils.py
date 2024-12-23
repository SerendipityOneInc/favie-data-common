import hashlib
import re
from collections.abc import Sized
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import tldextract
from dateutil import parser
from pydantic import BaseModel


class SerializeWrapper(BaseModel):
    datas: Optional[Any] = None


class CommonUtils:
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
    def host_trip_www(host: str):
        return re.sub(r"^www\.", "", host) if host is not None else None

    @staticmethod
    def md5_hash(text: str):
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

    @staticmethod
    def datetime_string_to_timestamp(date_string: str, assume_utc: bool = True) -> float:
        """
        将日期时间字符串转换为 UNIX 时间戳。

        支持的格式包括但不限于:
        - "2024-08-29T10:17:21.164262Z"
        - "2024-08-29T10:17:21.164262+00:00"
        - "2024-08-29T10:17:21"

        :param date_string: 日期时间字符串
        :param assume_utc: 如果为 True，没有时区信息的字符串将被假定为 UTC 时间
        :return: UNIX 时间戳（浮点数）
        """
        try:
            if not date_string:
                return None

            # 使用 dateutil 解析时间字符串
            dt = parser.parse(date_string)

            # 处理时区
            if dt.tzinfo is None:
                if assume_utc:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    raise ValueError("时间字符串没有时区信息，且 assume_utc 为 False")
            else:
                dt = dt.astimezone(timezone.utc)

            # 返回时间戳
            return dt.timestamp()
        except ValueError as e:
            raise ValueError(f"无法解析时间字符串: {date_string}. 错误: {str(e)}")

    @staticmethod
    def divide_chunks(lst, n):
        # 计算每个分片应有的长度
        chunk_size = len(lst) // n + (1 if len(lst) % n > 0 else 0)
        # 生成分片
        return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]

    @staticmethod
    def get_hostname(url):
        """获取URL的主机名"""
        parsed_url = urlparse(url)
        return parsed_url.hostname

    @staticmethod
    def get_domain(url):
        """获取URL的域名"""
        ext = tldextract.extract(url)
        domain = ext.domain + "." + ext.suffix
        return domain

    @staticmethod
    def get_subdomain(url):
        """获取主机名对应的域名"""
        ext = tldextract.extract(url)
        return ext.subdomain

    @staticmethod
    def get_full_subdomain(url):
        sub_domain = CommonUtils.get_subdomain(url)
        domain = CommonUtils.get_domain(url)
        return f"{sub_domain}.{domain}" if sub_domain else domain

    @staticmethod
    def serialize(datas):
        list_wrapper = SerializeWrapper(datas=datas)
        return list_wrapper.model_dump_json(exclude_none=True)[9:-1]

    @staticmethod
    def reverse_hostname_and_remove_http(url):
        if url is None:
            return None

        if not url.startswith("http"):
            url = "http://" + url
        # 解析 URL
        parsed_url = urlparse(url)

        # 获得并反转主机名
        hostname_parts = parsed_url.hostname.split(".")
        reversed_hostname = ".".join(reversed(hostname_parts))

        # 创建新的 URL，带有反转后的主机名
        new_netloc = reversed_hostname
        if parsed_url.port:
            new_netloc += f":{parsed_url.port}"

        # 将修改后的各部分组合成完整 URL
        new_url = urlunparse(
            ("", new_netloc, parsed_url.path, parsed_url.params, parsed_url.query, parsed_url.fragment)
        )
        if new_url.startswith("//"):
            new_url = new_url[2:]
        return new_url


if __name__ == "__main__":
    print(CommonUtils.get_full_subdomain("www.shop.lululemon.com:80"))
    print(CommonUtils.get_subdomain("shop.lululemon.com"))
    print(CommonUtils.serialize([{"shop.lululemon.com": 1, "www.shop.lululemon.com": "2"}, "hello"]))
