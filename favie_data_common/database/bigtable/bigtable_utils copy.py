import json
from typing import get_args
from pydantic import BaseModel

from favie_data_common.common.common_utils import CommonUtils
from favie_data_common.common.pydantic_utils import PydanticUtils


class BigtableUtils:
    @staticmethod
    def gen_hash_rowkey(key:str):
        md5 = CommonUtils.md5_hash(key)
        return f'{md5[0:6]}-{key}'  
    
    @staticmethod    
    def pydantic_field_convert_str(param,force_dump_json :bool = False) -> str:
        if isinstance(param, (int, float, str, bool)):  # 原生对象
            return json.dumps(param) if force_dump_json else str(param)
        elif isinstance(param, BaseModel):  # Pydantic 对象
            return param.model_dump_json(exclude_none=True)
        elif isinstance(param, list):  # 列表
            json_strings = [BigtableUtils.pydantic_field_convert_str(item,True) for item in param]  # 递归处理列表中的每个
            return '[' + ', '.join(json_strings) + ']'  # 组合成一个合法的 JSON 字符串元素
        elif isinstance(param, dict):# 字典类型处理
            # 对字典中的每一个键值对进行递归处理，并将键和值都转换成字符串格式
            json_pairs = [
                f'{json.dumps(key)}: {BigtableUtils.pydantic_field_convert_str(value, True)}'
                for key, value in param.items()
            ]
            # 返回 JSON 对象字符串
            return '{' + ', '.join(json_pairs) + '}'
        else:
            raise TypeError(f"Unsupported type : {type(param)}")

    @staticmethod
    def str_convert_pydantic_field(string_data: str, data_type: type):        
        # 处理原生类型
        if data_type == int:
            return int(string_data)
        elif data_type == float:
            return float(string_data)
        elif data_type == str:
            return string_data
        elif data_type == bool:
            return string_data.lower() == "true"
            
        # 处理列表类型 必须先处理列表类型，因为列表类型是泛型
        if PydanticUtils.is_type_of_list(data_type):
            item_type = get_args(data_type)[0]
            if isinstance(string_data, str):
                items = json.loads(string_data)
            else:
                items = BigtableUtils.str_convert_pydantic_field(string_data, item_type)
            return [BigtableUtils.str_convert_pydantic_field(item, item_type) for item in items]
        
                # 处理字典类型
        if PydanticUtils.is_type_of_dict(data_type):
            key_type, value_type = get_args(data_type)  # 获取键和值的类型
            if isinstance(string_data, str):
                dict_items = json.loads(string_data)
            else:
                dict_items = BigtableUtils.str_convert_pydantic_field(string_data, value_type)  
            # 确保键和值均通过递归调用进行适当的数据转换
            return {BigtableUtils.str_convert_pydantic_field(key, key_type): BigtableUtils.str_convert_pydantic_field(value, value_type) for key, value in dict_items.items()}
        
        # 处理 Pydantic 对象
        if issubclass(data_type, BaseModel):
            if isinstance(string_data, str):
                return data_type.model_validate_json(string_data)
            elif isinstance(string_data, dict):
                return data_type(**string_data)
        
        raise TypeError(f"Unsupported type: {data_type}")    