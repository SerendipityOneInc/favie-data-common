import re
from functools import lru_cache

from business_rules.fields import FIELD_NO_INPUT, FIELD_NUMERIC, FIELD_SELECT, FIELD_SELECT_MULTIPLE, FIELD_TEXT
from business_rules.operators import (
    BooleanType,
    Decimal,
    NumericType,
    SelectMultipleType,
    SelectType,
    StringType,
    float_to_decimal,
    integer_types,
    type_operator,
)


# 定义缓存的正则匹配
@lru_cache(maxsize=1024)
def get_cached_regex(pattern: str):
    return re.compile(pattern)


class FavieStringType(StringType):
    @type_operator(FIELD_TEXT)
    def equal_to(self, other_string):
        return self.value == other_string

    @type_operator(FIELD_TEXT, label="Equal To (case insensitive)")
    def equal_to_case_insensitive(self, other_string):
        if self.value is None:
            return False
        return self.value.lower() == other_string.lower()

    @type_operator(FIELD_TEXT)
    def starts_with(self, other_string):
        if self.value is None:
            return False
        return self.value.startswith(other_string)

    @type_operator(FIELD_TEXT)
    def ends_with(self, other_string):
        if self.value is None:
            return False
        return self.value.endswith(other_string)

    @type_operator(FIELD_TEXT)
    def contains(self, other_string):
        if self.value is None:
            return False
        return other_string in self.value

    @type_operator(FIELD_TEXT)
    def matches_regex(self, regex):
        if self.value is None:
            return False
        return re.search(regex, self.value)

    # 要添加的操作符逻辑
    @type_operator(FIELD_TEXT)
    def matches_cached_regex(self, regex):
        regex = get_cached_regex(regex)
        return bool(regex.match(self.value))

    @type_operator(FIELD_NO_INPUT)
    def non_empty(self):
        return bool(self.value)


class FavieNumericType(NumericType):
    @staticmethod
    def _assert_valid_value_and_cast(value):
        if value is None:
            return None
        if isinstance(value, float):
            # In python 2.6, casting float to Decimal doesn't work
            return float_to_decimal(value)
        if isinstance(value, integer_types):
            return Decimal(value)
        if isinstance(value, Decimal):
            return value
        else:
            raise AssertionError("{0} is not a valid numeric type.".format(value))

    @type_operator(FIELD_NUMERIC)
    def equal_to(self, other_numeric):
        if self.value is None:
            return False
        return abs(self.value - other_numeric) <= self.EPSILON

    @type_operator(FIELD_NUMERIC)
    def greater_than(self, other_numeric):
        if self.value is None:
            return False
        return (self.value - other_numeric) > self.EPSILON

    @type_operator(FIELD_NUMERIC)
    def greater_than_or_equal_to(self, other_numeric):
        if self.value is None:
            return False
        return self.greater_than(other_numeric) or self.equal_to(other_numeric)

    @type_operator(FIELD_NUMERIC)
    def less_than(self, other_numeric):
        if self.value is None:
            return False
        return (other_numeric - self.value) > self.EPSILON

    @type_operator(FIELD_NUMERIC)
    def less_than_or_equal_to(self, other_numeric):
        if self.value is None:
            return False
        return self.less_than(other_numeric) or self.equal_to(other_numeric)


class FavieBooleanType(BooleanType):
    def _assert_valid_value_and_cast(self, value):
        if value is None:
            return False

        if type(value) != bool:
            raise AssertionError("{0} is not a valid boolean type".format(value))

        return value


class FavieSelectType(SelectType):
    def _assert_valid_value_and_cast(self, value):
        if value is None:
            return None

        if not hasattr(value, "__iter__"):
            raise AssertionError("{0} is not a valid select type".format(value))
        return value

    @type_operator(FIELD_SELECT, assert_type_for_arguments=False)
    def contains(self, other_value):
        if self.value is None:
            return False

        for val in self.value:
            if self._case_insensitive_equal_to(val, other_value):
                return True
        return False

    @type_operator(FIELD_SELECT, assert_type_for_arguments=False)
    def does_not_contain(self, other_value):
        if self.value is None:
            return True

        for val in self.value:
            if self._case_insensitive_equal_to(val, other_value):
                return False
        return True

    @type_operator(FIELD_SELECT, label="Contains Not", assert_type_for_arguments=False)
    def contains_not(self, other_value):
        if self.value is None:
            return True

        for val in self.value:
            if not self._case_insensitive_equal_to(val, other_value):
                return True
        return False


class FavieSelectMultipleType(SelectMultipleType):
    def _assert_valid_value_and_cast(self, value):
        if value is None:
            return None
        if not hasattr(value, "__iter__"):
            raise AssertionError("{0} is not a valid select multiple type".format(value))
        return value

    @type_operator(FIELD_SELECT_MULTIPLE)
    def contains_all(self, other_value):
        if self.value is None:
            return False
        select = SelectType(self.value)
        for other_val in other_value:
            if not select.contains(other_val):
                return False
        return True

    @type_operator(FIELD_SELECT_MULTIPLE)
    def is_contained_by(self, other_value):
        if self.value is None:
            return False
        other_select_multiple = SelectMultipleType(other_value)
        return other_select_multiple.contains_all(self.value)

    @type_operator(FIELD_SELECT_MULTIPLE)
    def shares_at_least_one_element_with(self, other_value):
        if self.value is None:
            return False
        select = SelectType(self.value)
        for other_val in other_value:
            if select.contains(other_val):
                return True
        return False

    @type_operator(FIELD_SELECT_MULTIPLE)
    def shares_exactly_one_element_with(self, other_value):
        if self.value is None:
            return False
        found_one = False
        select = SelectType(self.value)
        for other_val in other_value:
            if select.contains(other_val):
                if found_one:
                    return False
                found_one = True
        return found_one

    @type_operator(FIELD_SELECT_MULTIPLE)
    def shares_no_elements_with(self, other_value):
        return not self.shares_at_least_one_element_with(other_value)
