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
    def str_convert_pydantic_field(string_data, data_type: type):
        """
        递归解析字符串到实际的Python对象，支持基本类型、列表、字典、Pydantic模型
        """
        # 处理基本类型 (int, float, str, bool)
        if data_type == int:
            return int(string_data)
        elif data_type == float:
            return float(string_data)
        elif data_type == str:
            return string_data
        elif data_type == bool:
            return string_data.lower() == "true" if isinstance(string_data, str) else bool(string_data)

        # 处理列表类型 (List)
        if PydanticUtils.is_type_of_list(data_type):
            item_type = get_args(data_type)[0]

            items = json.loads(string_data) if isinstance(string_data, str) else string_data

            # 处理列表中的元素，并递归处理 BaseModel 或复杂类型
            return [
                BigtableUtils.str_convert_complex_type(item, item_type)
                for item in items
            ]

        # 处理字典类型 (Dict)
        if PydanticUtils.is_type_of_dict(data_type):
            key_type, value_type = get_args(data_type)

            dict_items = json.loads(string_data) if isinstance(string_data, str) else string_data

            # 递归处理字典中的键和值
            return {
                BigtableUtils.str_convert_pydantic_field(key, key_type): BigtableUtils.str_convert_complex_type(value, value_type)
                for key, value in dict_items.items()
            }

        # 如果是 Pydantic 模型 (BaseModel)
        if issubclass(data_type, BaseModel):
            if isinstance(string_data, str):
                return data_type.model_validate_json(string_data)  # 处理 JSON 字符串
            elif isinstance(string_data, dict):
                return data_type(**string_data)  # 处理字典格式的输入

        # 抛出类型错误
        raise TypeError(f"Unsupported type: {data_type}")

    @staticmethod
    def str_convert_complex_type(item, expected_type: type):
        """
        递归检查和转换复杂对象，包括列表、字典等复合结构，确保 Pydantic 模型可以正确处理
        """

        # 确认 expected_type 是一个类并且是 Pydantic 模型
        if isinstance(expected_type, type) and issubclass(expected_type, BaseModel):
            if isinstance(item, dict):
                return expected_type(**item)
            elif isinstance(item, str):
                return expected_type.model_validate_json(item)

        # 如果 item 是列表，确保 expected_type 是 List，再递归处理
        if isinstance(item, list):
            if PydanticUtils.is_type_of_list(expected_type):
                inner_type = get_args(expected_type)[0]
                return [BigtableUtils.str_convert_complex_type(i, inner_type) for i in item]
            else:
                raise TypeError(f"Expected list type but got {type(item)} for {expected_type}")

        # 如果 item 是字典，确保 expected_type 是 Dict，再递归处理
        if isinstance(item, dict):
            if PydanticUtils.is_type_of_dict(expected_type):
                key_type, value_type = get_args(expected_type)
                return {
                    BigtableUtils.str_convert_complex_type(k, key_type): BigtableUtils.str_convert_complex_type(v, value_type)
                    for k, v in item.items()
                }
            else:
                raise TypeError(f"Expected dict type but got {type(item)} for {expected_type}")

        # 处理基础数据类型
        return BigtableUtils.str_convert_pydantic_field(item, expected_type)