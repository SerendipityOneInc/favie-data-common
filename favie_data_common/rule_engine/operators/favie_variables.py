from business_rules.variables import (
    _rule_variable_wrapper,
    rule_variable,
    select_rule_variable
)
from business_rules.operators import (
    StringType,
    SelectType,
    SelectMultipleType,
    NumericType,
    BooleanType,
    string_types,
    float_to_decimal,
    integer_types,
    Decimal
)

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
            raise AssertionError("{0} is not a valid numeric type.".
                                 format(value))

class FavieBooleanType(BooleanType):
    def _assert_valid_value_and_cast(self, value):
        if value is None:
            return False
        
        if type(value) != bool:
            raise AssertionError("{0} is not a valid boolean type".
                                 format(value))
            
        return value
    
class FavieSelectType(SelectType):
    def _assert_valid_value_and_cast(self, value):
        if value is None:
            return None
        
        if not hasattr(value, '__iter__'):
            raise AssertionError("{0} is not a valid select type".
                                    format(value))
        return value
    
class FavieSelectMultipleType(SelectMultipleType):
    def _assert_valid_value_and_cast(self, value):
        if value is None:
            return None
        if not hasattr(value, '__iter__'):
            raise AssertionError("{0} is not a valid select multiple type".
                                 format(value))
        return value

def favie_numeric_rule_variable(label=None):
    return _rule_variable_wrapper(FavieNumericType, label)

def favie_string_rule_variable(label=None):
    return _rule_variable_wrapper(StringType, label)

def favie_boolean_rule_variable(label=None):
    return _rule_variable_wrapper(FavieBooleanType, label)

def _favie_select_rule_variable(label=None, options=None):
    return rule_variable(FavieSelectType, label=label, options=options)

def favie_select_rule_variable(label=None, options=None):
    def decorator(func):
        # 使用第三方的 select_rule_variable 装饰器
        wrapped_func = _favie_select_rule_variable(label, options)(func)
        # 这里可以加入自己的自定义逻辑，扩展原始装饰器的行为
        # 例如，增加调试信息或调整一些属性

        return wrapped_func
    
    if callable(label):
        # 如果是无参数调用
        return decorator(label)

    return decorator

def _favie_select_multiple_rule_variable(label=None, options=None):
    return rule_variable(SelectMultipleType, label=label, options=options)

def favie_select_multiple_rule_variable(label=None, options=None):
    def decorator(func):
        wrapped_func = _favie_select_multiple_rule_variable(label, options)(func)
        return wrapped_func

    if callable(label):
        return decorator(label)

    return decorator