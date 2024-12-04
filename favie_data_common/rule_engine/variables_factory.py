from typing import Dict, List, Set, Tuple, Type, Optional,Any, Union, get_type_hints
from business_rules.variables import (
    BaseVariables,
    numeric_rule_variable,
    string_rule_variable,
    boolean_rule_variable,
    select_rule_variable,
    select_multiple_rule_variable
)
from business_rules.operators import (
    BaseType,
    NumericType,
    StringType,
    BooleanType,
    SelectType,
    SelectMultipleType
)
from pydantic import BaseModel
from business_rules.fields import FIELD_TEXT
from favie_data_common.common.pydantic_utils import PydanticUtils
from favie_data_common.rule_engine.operators.favie_operators import (
    register_operator,
    matches_cached_regex,
    contains_not
)

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

register_operator(StringType,matches_cached_regex)
register_operator(SelectType,contains_not)

def favie_select_rule_variable(label=None, options=None):
    def decorator(func):
        # 使用第三方的 select_rule_variable 装饰器
        wrapped_func = select_rule_variable(label, options)(func)
        # 这里可以加入自己的自定义逻辑，扩展原始装饰器的行为
        # 例如，增加调试信息或调整一些属性

        return wrapped_func
    
    if callable(label):
        # 如果是无参数调用
        return decorator(label)

    return decorator

class VariablesFactory:
    """
    动态生成规则变量的工具类，支持普通字段、差异字段、嵌套字段、列表字段，以及动态计算字段（@property）。
    """
    @staticmethod
    def build_variables(pydantic_model: Type[BaseModel]):
        class DynamicVariables(BaseVariables):
            def __init__(self, new_obj: BaseModel, base_obj: Optional[BaseModel] = None):
                if not isinstance(new_obj, pydantic_model):
                    raise ValueError(f"`new_obj` 必须是 {pydantic_model} 的实例。")
                self.new_instance = new_obj
                self.base_instance = base_obj
                
            def select_rule_variable(label=None, options=None):
                if callable(label):
                    # 如果直接传入方法作为参数，可能是无参数装饰器的应用方式
                    return rule_variable(SelectType)(label)
                return rule_variable(SelectType, label=label, options=options)
            
            def select_multiple_rule_variable(label=None, options=None):
                if callable(label):
                    # 如果直接传入方法作为参数，可能是无参数装饰器的应用方式
                    return rule_variable(SelectMultipleType)(label)
                return rule_variable(SelectMultipleType, label=label, options=options)
                            
        def get_rule_variable(field_type: Any):
            """
            根据字段类型返回对应的规则变量装饰器。
            支持数字 (int, float)、字符串 (str)、布尔值 (bool)、以及 Optional 类型。
            不支持的类型直接返回 None。
            """
            # 处理 Optional[X] 类型 (Union[X, None])
            native_file_type = PydanticUtils.get_native_type(field_type)

            # 处理基础类型
            if native_file_type in [int, float]:
                return numeric_rule_variable
            elif native_file_type == str:
                return string_rule_variable
            elif native_file_type == bool:
                return boolean_rule_variable

            # 默认：不支持的字段类型
            return None


        def bind_field_as_rule_variable(field_name: str, field_type: Any):
            """绑定普通字段规则变量."""
            rule_variable = get_rule_variable(field_type)
            if not rule_variable:
                return None
            
            
            @rule_variable
            def getter(self:DynamicVariables, field_name=field_name) -> field_type:
                if self.new_instance:
                    return getattr(self.new_instance, field_name)
            setattr(DynamicVariables, field_name, getter)

        def bind_property_as_rule_variable(property_name: str, property_type: Any):
            """为 @property 动态生成规则变量."""
            rule_variable = get_rule_variable(property_type)
            if not rule_variable:
                return None
            
            @rule_variable
            def property_getter(self:DynamicVariables, property_name=property_name) -> property_type:
                if self.new_instance:
                    value = getattr(self.new_instance, property_name, None)
                    return value if value is not None else (0 if property_type in [int, float] else "")
            setattr(DynamicVariables, property_name, property_getter)

        def bind_field_diff_as_rule_variable(field_name: str, field_type: Any):
            """绑定差异字段规则变量。"""
            native_faile_type = PydanticUtils.get_native_type(field_type)
            
            @numeric_rule_variable
            def diff_field(self: DynamicVariables):
                if not pydantic_model:
                    return 0
                base_value = getattr(self.base_instance, field_name, None)
                new_value = getattr(self.new_instance, field_name, None)

                # 差异计算
                if native_faile_type in [int, float]:
                    return (new_value or 0) - (base_value or 0)
                if native_faile_type == str:
                    return len(new_value or "") - len(base_value or "")
                if native_faile_type == list or native_faile_type == List:
                    if not base_value:
                        return len(new_value or [])
                    if not new_value:
                        return -len(base_value)
                    return len(new_value) - len(base_value)
                if native_faile_type == set or native_faile_type == Set:
                    base_set = base_value or set()
                    new_set = new_value or set()
                    # 计算集合的差异个数
                    return len(new_set - base_set)
                if native_faile_type == dict or native_faile_type == Dict:
                    base_dict = base_value or {}
                    new_dict = new_value or {}
                    # 可以计算键的差异个数
                    return len(set(new_dict.keys())) - len(set(base_dict.keys()))
                if native_faile_type == tuple or native_faile_type == Tuple:
                    if not base_value:
                        return len(new_value or ())
                    if not new_value:
                        return -len(base_value)
                    return len(new_value) - len(base_value)
                
                return 0


            setattr(DynamicVariables, f"{field_name}_diff", diff_field)


        def bind_pydantic_field(field_name: str, field_type: Type[BaseModel]):
            """
            仅处理 Pydantic 类型对象，生成形式为 fieldB_xxx 的变量。
            """
            if not PydanticUtils.is_type_of_pydantic_class(field_type):
                return None
            
            # 生成所有属性为基础类型的规则变量
            for sub_field_name, sub_field_type in PydanticUtils.get_fields_of_pydantic_class(field_type):
                variable_name = f"{field_name}_{sub_field_name}"
                native_sub_field_type = PydanticUtils.get_native_type(sub_field_type)
                if native_sub_field_type in [int, float, str, bool]:
                    rule_variable = get_rule_variable(native_sub_field_type)
                    if not rule_variable:
                        continue
                    #通过闭包生成getter，因为闭包是在循环中，需要使用默认参数，不然会出现变量名重复的问题
                    def make_getter(sub_field_name):
                        @rule_variable
                        def nested_getter(self: DynamicVariables):
                            nested_obj = getattr(self.new_instance, field_name, None)
                            if nested_obj:
                                sub_value = getattr(nested_obj, sub_field_name, None)
                                return sub_value
                            return None
                        return nested_getter                 
                    setattr(DynamicVariables, variable_name, make_getter(sub_field_name))
                if PydanticUtils.is_type_of_list(native_sub_field_type):
                    @favie_select_rule_variable  # 用于规则引擎集合操作
                    def list_getter(self:DynamicVariables)->list:
                        nested_list = getattr(self.new_instance, field_name, [])
                        result = []
                        if not nested_list:
                            return result
                        for nested_item in nested_list:
                            if nested_item:
                                if isinstance(nested_item, BaseModel):
                                    result.append(nested_item.model_dump())  # Pydantic v2
                                else:
                                    result.append(nested_item)  # 非 BaseModel，直接返回原始对象                    
                        return result
                    list_getter.field_type = SelectType
                    setattr(DynamicVariables, variable_name, list_getter)

        def bind_list_field(field_name: str, field_type: Any):
            """
            动态绑定列表字段（List[BaseModel] 类型）。
            此方法针对 `List[BaseModel]`，允许规则引擎对列表字段进行集合操作（如 contains）。
            """
            # 检查是否为 List
            if not PydanticUtils.is_type_of_list(field_type):
                return None
            
            @favie_select_rule_variable  # 用于规则引擎集合操作
            def list_getter(self:DynamicVariables)->list:# -> list:
                nested_list = getattr(self.new_instance, field_name, [])
                result = []
                if not nested_list:
                    return result
                for nested_item in nested_list:
                    if nested_item:
                        if isinstance(nested_item, BaseModel):
                            result.append(nested_item.model_dump())  # Pydantic v2
                        else:
                            result.append(nested_item)  # 非 BaseModel，直接返回原始对象                    
                return result
            list_getter.field_type = SelectType
            setattr(DynamicVariables, field_name, list_getter)
            
        def bind_list_field_by_subfield(field_name: str, field_type: Any):
            if not PydanticUtils.is_type_of_list(field_type):
                return None
            
            item_type = PydanticUtils.get_list_item_type(field_type)         
            if not PydanticUtils.is_type_of_pydantic_class(item_type):
                return None
            
            for sub_field_name, _ in PydanticUtils.get_fields_of_pydantic_class(item_type):
                def mark_list_field_getter(field_name, sub_field_name):
                    @favie_select_rule_variable  # 用于规则引擎集合操作
                    def list_field_getter(self:DynamicVariables)->list:# -> list:
                        nested_list = getattr(self.new_instance, field_name, [])
                        result = []
                        if not nested_list:
                            return result
                        for nested_item in nested_list:
                            if nested_item:
                                value = getattr(nested_item,sub_field_name)
                                if value:
                                    result.append(value)                   
                        return result
                    list_field_getter.field_type = SelectType
                    return list_field_getter
                setattr(DynamicVariables, f"{field_name}_{sub_field_name}" ,mark_list_field_getter(field_name, sub_field_name))           

        # Step 1: 注册普通字段规则变量（只处理普通字段，不包括 @property）
        for field_name, field_info in pydantic_model.model_fields.items():
            field_type = PydanticUtils.get_native_field_type(pydantic_model, field_name)
            bind_field_as_rule_variable(field_name, field_type)
            bind_pydantic_field(field_name, field_type)
            bind_list_field(field_name, field_type)
            bind_list_field_by_subfield(field_name, field_type)

        # Step 2: 注册 @property 字段规则变量
        for property_name in dir(pydantic_model):
            prop = getattr(pydantic_model, property_name)  # 获取属性
            if isinstance(prop, property):  # 如果是 @property
                # 直接从 fget 的返回值获取类型注解
                property_type = get_type_hints(prop.fget).get("return", None)
                if property_type:
                    bind_property_as_rule_variable(property_name, property_type)

        # Step 3: 注册差异规则变量（包括普通字段和 @property）
        for rule_variable_name in dir(DynamicVariables):
            if not rule_variable_name.startswith("_"):  # 忽略内置字段
                rule_variable = getattr(DynamicVariables, rule_variable_name, None)
                if callable(rule_variable):
                    # 获取规则变量的返回类型，并生成差异规则
                    rule_variable_type = get_type_hints(rule_variable).get("return", None)
                    if rule_variable_type:
                        bind_field_diff_as_rule_variable(rule_variable_name, rule_variable_type)

        return DynamicVariables
