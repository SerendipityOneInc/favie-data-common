import json
from typing import Dict, Optional, Set, Tuple, Union, List,Any
from typing import List, get_args, get_origin
from pydantic import BaseModel
from favie_data_common.common.common_utils import CommonUtils

class PydanticUtils:    
    @staticmethod
    def is_type_of_list(data_type: type):
        origin_type = get_origin(data_type)
        return origin_type == list or origin_type == List
    
    @staticmethod
    def is_type_of_dict(data_type: type):
        origin_type = get_origin(data_type)
        return origin_type == dict or origin_type == Dict
    
    @staticmethod
    def is_type_of_set(data_type: type) -> bool:
        origin_type = get_origin(data_type)
        return origin_type == set or origin_type == Set

    @staticmethod
    def is_type_of_tuple(data_type: type) -> bool:
        origin_type = get_origin(data_type)
        return origin_type == tuple or origin_type == Tuple
    
    @staticmethod
    def is_type_of_pydantic_class(data_type: type) -> bool:
        try:
            if data_type is None:
                return False
            return issubclass(data_type, BaseModel)
        except TypeError:
            return False
    
    @staticmethod
    def get_fields_of_pydantic_class(data_type: type) -> List[str]:
        if not PydanticUtils.is_type_of_pydantic_class(data_type):
            return []
        return [(field_name,field_type.annotation) for field_name,field_type in data_type.model_fields.items()]
        
    @staticmethod
    def get_list_item_type(field_type):
        # 确认 field_type 是一个泛型类型并且起源是 list
        if get_origin(field_type) in (list, List):
            # 获取参数类型 (即 list 的 item 类型)
            item_types = get_args(field_type)
            if item_types:
                return item_types[0]  # 通常 List 会仅有一个参数
        return None
    
    #获取字段类型
    @staticmethod
    def get_native_field_type(model: BaseModel, field_name: str):
        type = model.model_fields.get(field_name)
        native_type = PydanticUtils.get_native_type(type.annotation if type else None)
        return native_type
    
    #获取原生类型
    @staticmethod
    def get_native_type(optional_type):
        if hasattr(optional_type, '__origin__') and optional_type.__origin__ is Union:
            args = optional_type.__args__
            native_types = [arg for arg in args if arg is not type(None)]
            if native_types:
                return native_types[0]
        return optional_type
        
    @staticmethod
    def merge_object(*, source_obj: Optional[BaseModel], dest_obj: Optional[BaseModel], merge_fields: list[str] = None, deep_merge_fields: list[str] = None) -> Optional[BaseModel]:
        """
            source_obj : source object for merging
            dest_obj : target object for merging
            merge_fields : merge based on designated fields
            deep_merge_fields : fields requiring deep merge(only one level deep)
        """
        if dest_obj is None:
            return source_obj
        if source_obj is None:
            return dest_obj
        if not isinstance(source_obj, type(dest_obj)):
            return None
        
        fields_to_merge = merge_fields if CommonUtils.not_empty(merge_fields) else getattr(source_obj, '__dict__', {}).keys()

        for field_name in fields_to_merge:
            if hasattr(source_obj, field_name):
                source_value = getattr(source_obj, field_name)
                if source_value is not None:
                    if deep_merge_fields and field_name in deep_merge_fields and isinstance(source_value, BaseModel):
                        dest_value = getattr(dest_obj, field_name, None)
                        merged_value = PydanticUtils.merge_object(source_obj=source_value, dest_obj=dest_value)
                        setattr(dest_obj, field_name, merged_value)
                    else:
                        setattr(dest_obj, field_name, source_value)
        
        return dest_obj
    

    @staticmethod
    def deserialize_data(expected_type: Any, value: Any) -> Any:
        if value is None:
            return None

        expected_type = PydanticUtils.get_native_type(expected_type)
        # 检查并转换基本类型
        if expected_type in {int, float, str, bool}:
            try:
                return expected_type(value)
            except ValueError:
                raise ValueError(f"Cannot convert value '{value}' to {expected_type.__name__}")

        # 处理字符串形式的复杂结构
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON string for deserializing {expected_type}")

        if PydanticUtils.is_type_of_pydantic_class(expected_type):
            # 对 Pydantic 模型进行处理
            if isinstance(value, dict):
                fields = PydanticUtils.get_fields_of_pydantic_class(expected_type)
                return expected_type(**{field_name : PydanticUtils.deserialize_data(field_type, value.get(field_name)) for field_name, field_type in fields})
            else:
                return value
        
        # 检查并处理复杂类型
        if PydanticUtils.is_type_of_list(expected_type):
            item_type = get_args(expected_type)[0]
            return [PydanticUtils.deserialize_data(item_type, item) for item in value]
        elif PydanticUtils.is_type_of_set(expected_type):
            item_type = get_args(expected_type)[0]
            return {PydanticUtils.deserialize_data(item_type, item) for item in value}
        elif PydanticUtils.is_type_of_tuple(expected_type):
            item_types = get_args(expected_type)
            return tuple(PydanticUtils.deserialize_data(item_type, item) for item_type, item in zip(item_types, value))
        elif PydanticUtils.is_type_of_dict(expected_type):
            key_type, val_type = get_args(expected_type)
            return {PydanticUtils.deserialize_data(key_type, k): PydanticUtils.deserialize_data(val_type, v) for k, v in value.items()}

        # 默认返回原始值
        return value
        