from business_rules.variables import (
    _rule_variable_wrapper,
    rule_variable
)

from favie_data_common.rule_engine.operators.favie_operators import (
    FavieBooleanType,
    FavieNumericType,
    FavieSelectMultipleType,
    FavieSelectType,
    FavieStringType
)

def favie_numeric_rule_variable(label=None):
    return _rule_variable_wrapper(FavieNumericType, label)

def favie_string_rule_variable(label=None):
    return _rule_variable_wrapper(FavieStringType, label)

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
    return rule_variable(FavieSelectMultipleType, label=label, options=options)

def favie_select_multiple_rule_variable(label=None, options=None):
    def decorator(func):
        wrapped_func = _favie_select_multiple_rule_variable(label, options)(func)
        return wrapped_func

    if callable(label):
        return decorator(label)

    return decorator