
from business_rules.operators import StringType
from business_rules.fields import FIELD_TEXT

def register_operator_to_string_type(name, operator_func, input_type, label=None):
    """
    动态添加一个新的操作符到 `StringType`
    :param name: 操作符名称（方法名）
    :param operator_func: 操作符逻辑（函数）
    :param input_type: 输入类型，例如 FIELD_TEXT
    :param label: 可选的 UI 显示标签，默认等同 `name`
    """
    # 设置操作符的元信息
    operator_func.is_operator = True
    operator_func.label = label or name.replace('_', ' ').capitalize()
    operator_func.input_type = input_type

    # 将操作符绑定到 `StringType`
    setattr(StringType, name, operator_func)
    


