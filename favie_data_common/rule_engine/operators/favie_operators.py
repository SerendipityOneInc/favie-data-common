import re
from functools import lru_cache
from business_rules.operators import (
    StringType,
    SelectType
)
from business_rules.fields import (
    FIELD_TEXT,
    FIELD_SELECT
)
from business_rules.operators import type_operator

def register_operator(type,operator_func):
    setattr(type,operator_func.__name__,operator_func)

# 定义缓存的正则匹配
@lru_cache(maxsize=1024)
def get_cached_regex(pattern: str):
    return re.compile(pattern)

# 要添加的操作符逻辑
@type_operator(FIELD_TEXT,label='Matches Cached Regex')
def matches_cached_regex(self, regex):
    regex = get_cached_regex(regex)
    return bool(regex.match(self.value))

@type_operator(FIELD_SELECT, label='Contains Not', assert_type_for_arguments=False)
def contains_not(self, other_value):
    for val in self.value:
        if not self._case_insensitive_equal_to(val, other_value):
            return True
    return False

