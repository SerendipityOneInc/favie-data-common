from typing import Type, Optional
from business_rules.variables import (
    BaseVariables,
    numeric_rule_variable,
    string_rule_variable,
    boolean_rule_variable,
)
from pydantic import BaseModel
from business_rules.fields import FIELD_TEXT
from favie_data_common.rule_engine.operators.matches_cached_regex import matches_cached_regex
from favie_data_common.rule_engine.operators.operator_utils import register_operator_to_string_type

"""
    规则对应的Operator
    @numeric_rule_variable operators:
    equal_to
    greater_than
    less_than
    greater_than_or_equal_to
    less_than_or_equal_to

    @string_rule_variable operators:
    equal_to
    equal_to_case_insensitive
    starts_with
    ends_with
    contains
    matches_regex
    non_empty

    @boolean_rule_variable operators:
    is_true
    is_false

    @select_rule_variable operators:
    contains
    does_not_contain

    @select_multiple_rule_variable operators:
    contains_all
    is_contained_by
    shares_at_least_one_element_with
    shares_exactly_one_element_with
    shares_no_elements_with
"""

register_operator_to_string_type(
    name="matches_cached_regex",
    operator_func=matches_cached_regex,
    input_type=FIELD_TEXT,
    label="Matches Cached Regex"
)

class VariablesFactory:
    """
    动态生成规则变量和差异字段规则变量的静态工具类
    """
    @staticmethod
    def build_variables(pydantic_model: Type[BaseModel]):
        """
        动态生成字段规则变量类，并可选支持字段差异的差异字段变量
        :param pydantic_model: Pydantic 模型类型
        :return: 动态生成的规则变量类（支持 diff）
        """

        # 动态生成的规则变量类
        class DynamicVariables(BaseVariables):
            def __init__(self, new_obj: BaseModel, base_obj: Optional[BaseModel] = None):
                """
                初始化动态规则变量类
                :param new_obj: 新对象（BaseModel 实例）
                :param base_obj: 基准对象（可选，BaseModel 实例）
                """
                # 确保输入的对象与 pydantic_model 类型一致
                if not isinstance(new_obj, pydantic_model):
                    raise ValueError("`new_obj` must be an instance of the provided Pydantic model.")
                if base_obj and not isinstance(base_obj, pydantic_model):
                    raise ValueError("`base_obj` must be an instance of the provided Pydantic model.")
                
                # 存储新对象和基准对象
                self.new_instance = new_obj
                self.base_instance = base_obj

            # 动态装配每个字段的差异规则变量
            @classmethod
            def create_diff_field(cls, field_name, field_type):
                """
                创建一个动态差异规则变量方法，用于规则引擎调用
                """
                @numeric_rule_variable
                def diff_field(self):
                    # 如果基准对象不存在，则返回 None
                    if not self.base_instance:
                        return 0
                    
                    # 获取字段的旧值和新值
                    old_val = getattr(cls.base_instance, field_name, None)
                    new_val = getattr(cls.new_instance, field_name, None)
                    
                    if old_val is None or new_val is None:
                        return 0 

                    # 根据字段类型计算差异值
                    if field_type in [int, float]:
                        return new_val - old_val 
                    elif field_type == str:
                        # 对字符串计算长度差异
                        return len(new_val) - len(old_val) if isinstance(old_val, str) and isinstance(new_val, str) else 0
                    elif field_type in [list, set, dict, tuple]:
                        # 集合或序列类型计算长度差
                        return len(new_val) - len(old_val) 
                    elif field_type == bool:
                        # 布尔值转换为 0 和 1 进行差值运算
                        return int(new_val) - int(old_val) 
                    else:
                        # 默认其他类型返回 None，表示无法计算差异
                        return 0
                
                # 动态绑定差异方法到类
                setattr(cls, f"{field_name}_diff", diff_field)

        # 遍历所有字段，并为每个字段添加差异字段规则变量
        for field_name, field in pydantic_model.model_fields.items():
            field_type = field.annotation
            # 为字段生成差异变量
            DynamicVariables.create_diff_field(field_name, field_type)

        # 定义普通字段规则变量
        attributes = {}

        # 遍历 Pydantic 模型字段，动态生成普通规则变量
        for field_name, field in pydantic_model.model_fields.items():
            field_type = field.annotation

            # 数字类型规则变量
            if field_type in [int, float]:
                @numeric_rule_variable
                def getter(self, field_name=field_name):
                    return getattr(self.new_instance, field_name)
                attributes[field_name] = getter

            # 字符串类型规则变量
            elif field_type == str:
                @string_rule_variable
                def getter(self, field_name=field_name):
                    return getattr(self.new_instance, field_name)
                attributes[field_name] = getter

            # 布尔类型规则变量
            elif field_type == bool:
                @boolean_rule_variable
                def getter(self, field_name=field_name):
                    return getattr(self.new_instance, field_name)
                attributes[field_name] = getter

            # 其他字段规则变量（按字符串处理）
            else:
                @string_rule_variable
                def getter(self, field_name=field_name):
                    return getattr(self.new_instance, field_name)
                attributes[field_name] = getter

        # 处理动态属性 (如 Pydantic 的 @property)
        for attr_name in dir(pydantic_model):
            if not attr_name.startswith("_"):  # 排除特殊或私有属性
                attr = getattr(pydantic_model, attr_name)

                # 判断属性是否为 `property`
                if isinstance(attr, property):
                    @numeric_rule_variable
                    def dynamic_getter(self, attr_name=attr_name):
                        return getattr(self.new_instance, attr_name)
                    attributes[attr_name] = dynamic_getter

        # 将规则变量绑定到 DynamicVariables 类
        for field, getter_method in attributes.items():
            setattr(DynamicVariables, field, getter_method)

        return DynamicVariables
