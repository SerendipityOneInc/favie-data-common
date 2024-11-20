import json
from typing import get_args, Any
from pydantic import BaseModel

from favie_data_common.common.common_utils import CommonUtils
from favie_data_common.common.pydantic_utils import PydanticUtils

class BigtableUtils:
    @staticmethod
    def gen_hash_rowkey(key: str):
        # 假设 CommonUtils是你项目中的工具类实现了md5哈希方法
        md5 = CommonUtils.md5_hash(key)
        return f'{md5[0:6]}-{key}'  

    @staticmethod
    def pydantic_field_convert_str(param, force_dump_json: bool = False) -> str:
        """
        递归处理Pydantic字段，将字段转化为字符串
        """
        if isinstance(param, (int, float, str, bool)):  # 基础类型处理
            return json.dumps(param) if force_dump_json else str(param)
        elif isinstance(param, BaseModel):  # Pydantic 对象处理
            return param.model_dump_json(exclude_none=True)
        elif isinstance(param, list):  # 列表类型处理
            json_strings = [BigtableUtils.pydantic_field_convert_str(item, True) for item in param]
            return '[' + ', '.join(json_strings) + ']'
        elif isinstance(param, set):  # 集合类型处理 (转化为列表)
            json_strings = [BigtableUtils.pydantic_field_convert_str(item, True) for item in param]
            return '[' + ', '.join(json_strings) + ']'
        elif isinstance(param, tuple):  # 元组类型处理
            json_strings = [BigtableUtils.pydantic_field_convert_str(item, True) for item in param]
            return '[' + ', '.join(json_strings) + ']'
        elif isinstance(param, dict):  # 字典类型处理
            json_pairs = [
                f'{json.dumps(key)}: {BigtableUtils.pydantic_field_convert_str(value, True)}'
                for key, value in param.items()
            ]
            return '{' + ', '.join(json_pairs) + '}'
        else:
            raise TypeError(f"Unsupported type : {type(param)}")

    @staticmethod
    def str_convert_pydantic_field(string_data, data_type: type):
        """
        反序列化字符串到 Python 对象，支持基本类型、列表、字典、集合、元组、Pydantic 模型等
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

        # 如果类型是Any, 则直接返回原始数据，无需再做类型校验
        if data_type == Any or data_type == any:
            # 如果是字符串，先尝试将其解析为JSON
            try:
                parsed_data = json.loads(string_data)
                return parsed_data
            except (json.JSONDecodeError, TypeError):
                return string_data

        # 处理 List 类型
        if PydanticUtils.is_type_of_list(data_type):
            item_type = get_args(data_type)[0] if get_args(data_type) else Any
            items = json.loads(string_data) if isinstance(string_data, str) else string_data

            return [
                BigtableUtils.str_convert_complex_type(item, item_type)
                for item in items
            ]

        # 处理 Set 类型 (Set)
        if PydanticUtils.is_type_of_set(data_type):
            item_type = get_args(data_type)[0] if get_args(data_type) else Any
            items = json.loads(string_data) if isinstance(string_data, str) else string_data

            return {BigtableUtils.str_convert_complex_type(item, item_type) for item in items}

        # 处理 Tuple 类型 (Tuple)
        if PydanticUtils.is_type_of_tuple(data_type):
            item_types = get_args(data_type)  # 获取所预期元组结构的每个元素的类型
            items = json.loads(string_data) if isinstance(string_data, str) else string_data

            if len(item_types) != len(items):
                raise TypeError(f"Mismatch between tuple types and items: expected {len(item_types)} items, got {len(items)}")

            return tuple(
                BigtableUtils.str_convert_complex_type(item, item_types[index] if index < len(item_types) else Any)
                for index, item in enumerate(items)
            )

        # 处理 Dict 类型
        if PydanticUtils.is_type_of_dict(data_type):
            key_type, value_type = (get_args(data_type) or (Any, Any))  # 如果缺失`Any`作为默认
            dict_items = json.loads(string_data) if isinstance(string_data, str) else string_data

            return {
                BigtableUtils.str_convert_pydantic_field(key, key_type): BigtableUtils.str_convert_complex_type(value, value_type)
                for key, value in dict_items.items()
            }

        # 处理 Pydantic 模型 (BaseModel)
        if isinstance(data_type, type) and issubclass(data_type, BaseModel):
            if isinstance(string_data, str):
                return data_type.model_validate_json(string_data)
            elif isinstance(string_data, dict):
                return data_type(**string_data)

        raise TypeError(f"Unsupported type: {data_type}")

    @staticmethod
    def str_convert_complex_type(item, expected_type: type):
        """
        递归检查和转换复杂对象，包括列表、字典、集合、元组等复合结构，确保 Pydantic 模型可以正确处理
        """

        # 如果expected_type是Any，则使用item的动态类型
        if expected_type == Any:
            if isinstance(item, dict):
                return {
                    BigtableUtils.str_convert_complex_type(k, Any): BigtableUtils.str_convert_complex_type(v, Any)
                    for k, v in item.items()
                }
            elif isinstance(item, list):
                return [BigtableUtils.str_convert_complex_type(i, Any) for i in item]
            elif isinstance(item, set):
                return {BigtableUtils.str_convert_complex_type(i, Any) for i in item}
            elif isinstance(item, tuple):
                return tuple(BigtableUtils.str_convert_complex_type(i, Any) for i in item)
            else:
                return item

        # 处理 Pydantic 模型
        if isinstance(expected_type, type) and issubclass(expected_type, BaseModel):
            if isinstance(item, dict):
                return expected_type(**item)
            elif isinstance(item, str):
                return expected_type.model_validate_json(item)

        # 如果 item 是列表 (list), 继续递归
        if isinstance(item, list):
            if PydanticUtils.is_type_of_list(expected_type):
                inner_type = get_args(expected_type)[0] if get_args(expected_type) else Any
                return [BigtableUtils.str_convert_complex_type(i, inner_type) for i in item]
            else:
                raise TypeError(f"Expected list type but got {type(item)} for {expected_type}")

        # 如果 item 是集合 (set), 继续递归
        if isinstance(item, set):
            if PydanticUtils.is_type_of_set(expected_type):
                inner_type = get_args(expected_type)[0] if get_args(expected_type) else Any
                return {BigtableUtils.str_convert_complex_type(i, inner_type) for i in item}
            else:
                raise TypeError(f"Expected set type but got {type(item)} for {expected_type}")

        # 如果 item 是元组 (tuple), 继续递归
        if isinstance(item, tuple):
            if PydanticUtils.is_type_of_tuple(expected_type):
                item_types = get_args(expected_type) or (Any,) * len(item)

                if len(item_types) != len(item):
                    raise TypeError(f"Mismatch between tuple types and items: expected {len(item_types)} items, got {len(item)}")

                return tuple(
                    BigtableUtils.str_convert_complex_type(i, i_type)
                    for i, i_type in zip(item, item_types)
                )
            else:
                raise TypeError(f"Expected tuple type but got {type(item)} for {expected_type}")

        # 如果 item 是字典 (dict), 继续递归
        if isinstance(item, dict):
            if PydanticUtils.is_type_of_dict(expected_type):
                key_type, value_type = get_args(expected_type) or (Any, Any)
                return {
                    BigtableUtils.str_convert_complex_type(k, key_type): BigtableUtils.str_convert_complex_type(v, value_type)
                    for k, v in item.items()
                }
            else:
                raise TypeError(f"Expected dict type but got {type(item)} for {expected_type}")

        # 处理基础数据类型 或者 仍然为`expected_type`
        return BigtableUtils.str_convert_pydantic_field(item, expected_type)
