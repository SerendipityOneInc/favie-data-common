import json
import unittest
from typing import Any, Dict, List, Set, Tuple

from pydantic import BaseModel

from favie_data_common.database.bigtable.bigtable_utils import BigtableUtils  # 请确保导入正确


# Pydantic 模型
class TestModel(BaseModel):
    name: str
    age: int
    is_active: bool
    scores: List[float]


# 嵌套模型，用于测试复杂结构的模型
class TestNestedModel(BaseModel):
    id: int
    test_model: TestModel


class TestBigtableRepository(unittest.TestCase):
    def test_pydantic_field_convert_str(self):
        # 测试原生类型
        self.assertEqual(BigtableUtils.pydantic_field_convert_str(10), "10")
        self.assertEqual(BigtableUtils.pydantic_field_convert_str(10.5), "10.5")
        self.assertEqual(BigtableUtils.pydantic_field_convert_str("test"), "test")
        self.assertEqual(BigtableUtils.pydantic_field_convert_str(True), "True")

        # 测试 Pydantic 模型
        model = TestModel(name="John", age=30, is_active=True, scores=[9.5, 8.0])
        expected_json = '{"name":"John","age":30,"is_active":true,"scores":[9.5,8.0]}'
        self.assertEqual(BigtableUtils.pydantic_field_convert_str(model), expected_json)

        # 测试列表
        self.assertEqual(BigtableUtils.pydantic_field_convert_str([1, 2, 3]), "[1, 2, 3]")

        # 测试集合 (Set)
        test_set = {1, 2, 3}
        expected_set_json = "[1, 2, 3]"  # 集合会被转换成列表格式
        self.assertEqual(BigtableUtils.pydantic_field_convert_str(test_set), expected_set_json)

        # 测试元组 (Tuple)
        test_tuple = (10, "value", 30.5)
        expected_tuple_json = '[10, "value", 30.5]'
        self.assertEqual(BigtableUtils.pydantic_field_convert_str(test_tuple), expected_tuple_json)

        # 测试嵌套模型
        nested_model = TestNestedModel(id=1, test_model=model)
        expected_nested_json = '{"id":1,"test_model":{"name":"John","age":30,"is_active":true,"scores":[9.5,8.0]}}'
        self.assertEqual(BigtableUtils.pydantic_field_convert_str(nested_model), expected_nested_json)

        # 测试字典
        dictionary_data = {"key1": 123, "key2": [11, 22], "nested": {"inner_key": "value"}}
        expected_dict_json = '{"key1": 123, "key2": [11, 22], "nested": {"inner_key": "value"}}'
        self.assertEqual(BigtableUtils.pydantic_field_convert_str(dictionary_data), expected_dict_json)

        # 新增 Any 类型测试
        any_data = {"key1": 123, "key2": [11, 22], "any_field": True}
        expected_any_json = '{"key1": 123, "key2": [11, 22], "any_field": true}'
        self.assertEqual(BigtableUtils.pydantic_field_convert_str(any_data), expected_any_json)

        # 新增 List[List[BaseModel]] 测试
        list_of_models = [
            [TestModel(name="Inner1", age=20, is_active=True, scores=[89.0])],
            [TestModel(name="Inner2", age=25, is_active=False, scores=[92.5])],
        ]
        expected_list_json = '[ [{"name":"Inner1","age":20,"is_active":true,"scores":[89.0]}], [{"name":"Inner2","age":25,"is_active":false,"scores":[92.5]}] ]'
        self.assertEqual(
            json.loads(BigtableUtils.pydantic_field_convert_str(list_of_models, True)), json.loads(expected_list_json)
        )

        # 新增 Dict[str, List[BaseModel]] 测试
        dict_of_models = {
            "group1": [TestModel(name="A", age=18, is_active=True, scores=[90.0])],
            "group2": [TestModel(name="B", age=22, is_active=False, scores=[95.5])],
        }
        expected_dict_of_models_json = '{"group1": [{"name":"A","age":18,"is_active":true,"scores":[90.0]}], "group2": [{"name":"B","age":22,"is_active":false,"scores":[95.5]}]}'
        self.assertEqual(
            json.loads(BigtableUtils.pydantic_field_convert_str(dict_of_models, True)),
            json.loads(expected_dict_of_models_json),
        )

        # 新增 List[Dict[str, List[BaseModel]]] 测试 —— 更复杂的嵌套结构
        list_of_dict_of_models = [
            {
                "group1": [TestModel(name="AA", age=30, is_active=True, scores=[100.0])],
                "group2": [TestModel(name="BB", age=40, is_active=False, scores=[80.0])],
            },
            {
                "group3": [TestModel(name="CC", age=50, is_active=True, scores=[70.0])],
                "group4": [TestModel(name="DD", age=60, is_active=False, scores=[60.0])],
            },
        ]
        expected_list_dict_json = '[{"group1": [{"name": "AA", "age": 30, "is_active": true, "scores": [100.0]}], "group2": [{"name": "BB", "age": 40, "is_active": false, "scores": [80.0]}]}, {"group3": [{"name": "CC", "age": 50, "is_active": true, "scores": [70.0]}], "group4": [{"name": "DD", "age": 60, "is_active": false, "scores": [60.0]}]}]'
        self.assertEqual(
            json.loads(BigtableUtils.pydantic_field_convert_str(list_of_dict_of_models, True)),
            json.loads(expected_list_dict_json),
        )

        # 测试不支持的类型 (比如 frozenset)
        with self.assertRaises(TypeError):
            BigtableUtils.pydantic_field_convert_str(frozenset([1, 2, 3]))

    def test_str_convert_pydantic_field(self):
        # 测试原生类型
        self.assertEqual(BigtableUtils.str_convert_pydantic_field("10", int), 10)
        self.assertEqual(BigtableUtils.str_convert_pydantic_field("10.5", float), 10.5)
        self.assertEqual(BigtableUtils.str_convert_pydantic_field("test", str), "test")
        self.assertEqual(BigtableUtils.str_convert_pydantic_field("true", bool), True)

        # 测试列表
        self.assertEqual(BigtableUtils.str_convert_pydantic_field("[9.5, 8.0]", List[float]), [9.5, 8.0])

        # 测试集合 (Set)
        set_str = "[1, 2, 3]"
        self.assertEqual(BigtableUtils.str_convert_pydantic_field(set_str, Set[int]), {1, 2, 3})

        # 测试元组 (Tuple)
        tuple_str = '[10, "value", 30.5]'
        self.assertEqual(
            BigtableUtils.str_convert_pydantic_field(tuple_str, Tuple[int, str, float]), (10, "value", 30.5)
        )

        # 测试 Pydantic 模型
        json_str = '{"name": "John", "age": 30, "is_active": true, "scores": [9.5, 8.0]}'
        model = BigtableUtils.str_convert_pydantic_field(json_str, TestModel)
        self.assertIsInstance(model, TestModel)
        self.assertEqual(model.name, "John")
        self.assertEqual(model.age, 30)
        self.assertEqual(model.is_active, True)
        self.assertEqual(model.scores, [9.5, 8.0])

        # 测试嵌套模型
        nested_json_str = (
            '{"id": 1, "test_model": {"name": "John", "age": 30, "is_active": true, "scores": [9.5, 8.0]}}'
        )
        nested_model = BigtableUtils.str_convert_pydantic_field(nested_json_str, TestNestedModel)
        self.assertIsInstance(nested_model, TestNestedModel)
        self.assertEqual(nested_model.id, 1)
        self.assertIsInstance(nested_model.test_model, TestModel)
        self.assertEqual(nested_model.test_model.name, "John")

        # 测试字典
        dict_str = '{"key1": 123, "key2": 11, "nested": 10}'
        dict_type = Dict[str, int]  # 假设键和值类型都为字符串的字典
        dictionary_data = BigtableUtils.str_convert_pydantic_field(dict_str, dict_type)
        expected_dict = {"key1": 123, "key2": 11, "nested": 10}
        self.assertEqual(dictionary_data, expected_dict)

        # 测试 Any 类型
        any_data_str = '{"key": "value", "number": 1234, "list_data": [1, 2, 3], "obj_data": {"name": "test"}}'
        any_data_type = Dict[str, Any]
        any_data_result = BigtableUtils.str_convert_pydantic_field(any_data_str, any_data_type)

        self.assertEqual(any_data_result["key"], "value")
        self.assertEqual(any_data_result["number"], 1234)
        self.assertEqual(any_data_result["list_data"], [1, 2, 3])
        self.assertEqual(any_data_result["obj_data"]["name"], "test")

        # 测试 List[List[BaseModel]] 类型
        list_str = '[ [{"name": "Inner1", "age": 20, "is_active": true, "scores": [89.0]}], [{"name": "Inner2", "age": 25, "is_active": false, "scores": [92.5]}] ]'
        list_type = List[List[TestModel]]
        list_result = BigtableUtils.str_convert_pydantic_field(list_str, list_type)

        self.assertEqual(list_result[0][0].name, "Inner1")
        self.assertEqual(list_result[1][0].name, "Inner2")

        # 测试 Dict[str, List[BaseModel]] 类型
        dict_of_models_str = '{"group1": [{"name": "A", "age": 18, "is_active": true, "scores": [90.0]}], "group2": [{"name": "B", "age": 22, "is_active": false, "scores": [95.5]}]}'
        dict_of_models_type = Dict[str, List[TestModel]]
        dict_of_models_result = BigtableUtils.str_convert_pydantic_field(dict_of_models_str, dict_of_models_type)

        self.assertEqual(dict_of_models_result["group1"][0].name, "A")
        self.assertEqual(dict_of_models_result["group2"][0].name, "B")

        # 测试 List[Dict[str, List[BaseModel]]] 类型
        complex_list_dict_str = '[{"group1": [{"name": "AA", "age": 30, "is_active": true, "scores": [100.0]}], "group2": [{"name": "BB", "age": 40, "is_active": false, "scores": [80.0]}]}, {"group3": [{"name": "CC", "age": 50, "is_active": true, "scores": [70.0]}], "group4": [{"name": "DD", "age": 60, "is_active": false, "scores": [60.0]}]}]'
        complex_list_dict_type = List[Dict[str, List[TestModel]]]
        complex_list_dict_result = BigtableUtils.str_convert_pydantic_field(
            complex_list_dict_str, complex_list_dict_type
        )

        self.assertEqual(complex_list_dict_result[0]["group1"][0].name, "AA")
        self.assertEqual(complex_list_dict_result[1]["group3"][0].name, "CC")

        # 测试不支持的类型 (比如 frozenset)
        with self.assertRaises(TypeError):
            BigtableUtils.str_convert_pydantic_field("test", frozenset)


if __name__ == "__main__":
    unittest.main()
