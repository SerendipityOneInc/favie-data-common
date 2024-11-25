import re
from functools import lru_cache

# 定义缓存的正则匹配
@lru_cache(maxsize=1024)
def get_cached_regex(pattern: str):
    return re.compile(pattern)

# 要添加的操作符逻辑
def matches_cached_regex(self, regex):
    regex = get_cached_regex(regex)
    return bool(regex.match(self.value))

# print(callable(matches_cached_regex))

