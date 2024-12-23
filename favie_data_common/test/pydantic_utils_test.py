import json
import unittest
from typing import Dict, List, Optional

from pydantic import BaseModel, field_validator

from favie_data_common.common.pydantic_utils import PydanticUtils  # 请替换为实际的导入路径


class TestModel(BaseModel):
    name: Optional[str] = None
    age: Optional[int] = None
    address: Optional["Address"] = None


class ComplexTestModel(BaseModel):
    id: Optional[int] = None
    data: Optional[TestModel] = None
    tags: Optional[List[str]] = None


class Address(BaseModel):
    city: Optional[str] = None
    street: Optional[str] = None
    zip_code: Optional[str] = None


# 一个简单的 Pydantic 模型
class Product(BaseModel):
    id: Optional[int] = None

    @field_validator("id", mode="before")
    def validate_id(cls, value):
        expected_type = PydanticUtils.get_native_field_type(cls, "id")
        return PydanticUtils.deserialize_data(expected_type, value)

    name: Optional[str] = None

    @field_validator("name", mode="before")
    def validate_name(cls, value):
        expected_type = PydanticUtils.get_native_field_type(cls, "name")
        return PydanticUtils.deserialize_data(expected_type, value)

    colors: Optional[List[str]] = None

    @field_validator("colors", mode="before")
    def validate_colors(cls, value):
        expected_type = PydanticUtils.get_native_field_type(cls, "colors")
        return PydanticUtils.deserialize_data(expected_type, value)

    addresses: Optional[List[Address]] = None

    @field_validator("addresses", mode="before")
    def validate_addresses(cls, value):
        expected_type = PydanticUtils.get_native_field_type(cls, "addresses")
        return PydanticUtils.deserialize_data(expected_type, value)

    main_address: Optional[Address] = None

    @field_validator("main_address", mode="before")
    def validate_main_address(cls, value):
        expected_type = PydanticUtils.get_native_field_type(cls, "main_address")
        return PydanticUtils.deserialize_data(expected_type, value)

    attributes: Optional[Dict[str, str]] = None

    @field_validator("attributes", mode="before")
    def validate_attributes(cls, value):
        expected_type = PydanticUtils.get_native_field_type(cls, "attributes")
        return PydanticUtils.deserialize_data(expected_type, value)


class TestPydanticUtils(unittest.TestCase):
    def test_is_type_of_list(self):
        self.assertTrue(PydanticUtils.is_type_of_list(List[int]))
        self.assertTrue(PydanticUtils.is_type_of_list(List[str]))
        self.assertFalse(PydanticUtils.is_type_of_list(int))
        self.assertFalse(PydanticUtils.is_type_of_list(str))

    def test_is_type_of_dict(self):
        self.assertTrue(PydanticUtils.is_type_of_dict(dict[any, any]))
        self.assertTrue(PydanticUtils.is_type_of_dict(dict[str, int]))
        self.assertFalse(PydanticUtils.is_type_of_dict(int))
        self.assertFalse(PydanticUtils.is_type_of_dict(str))

    def test_get_field_type(self):
        self.assertEqual(PydanticUtils.get_native_field_type(TestModel, "name"), str)
        self.assertEqual(PydanticUtils.get_native_field_type(TestModel, "age"), int)
        self.assertIsNone(PydanticUtils.get_native_field_type(TestModel, "non_existent_field"))

    def test_get_native_type(self):
        self.assertEqual(PydanticUtils.get_native_type(Optional[int]), int)
        self.assertEqual(PydanticUtils.get_native_type(int), int)
        self.assertEqual(PydanticUtils.get_native_type(Optional[List[str]]), List[str])

    def test_is_simple_type(self):
        self.assertTrue(PydanticUtils.is_simple_type(int))
        self.assertTrue(PydanticUtils.is_simple_type(str))
        self.assertTrue(PydanticUtils.is_simple_type(float))
        self.assertTrue(PydanticUtils.is_simple_type(bool))
        self.assertFalse(PydanticUtils.is_simple_type(List[int]))
        self.assertFalse(PydanticUtils.is_simple_type(Dict[str, int]))

    def test_merge_object(self):
        source = TestModel(name="Alice", age=30)
        dest = TestModel(name="Bob", age=None)

        merged = PydanticUtils.merge_object(source_obj=source, dest_obj=dest)
        self.assertEqual(merged.name, "Alice")
        self.assertEqual(merged.age, 30)

        # 测试深度合并
        source_complex = ComplexTestModel(
            id=1,
            data=TestModel(name="Alice", age=30, address=Address(city="Shanghai", street="Nanjing Road")),
            tags=["tag1"],
        )
        dest_complex = ComplexTestModel(
            id=2,
            data=TestModel(name="Bob", age=25, address=Address(city="Beijing", street="Wangfujing", zip_code="100000")),
            tags=["tag2"],
        )

        merged_complex = PydanticUtils.merge_object(
            source_obj=source_complex, dest_obj=dest_complex, deep_merge_config={"data": {"address": {}}}
        )
        self.assertEqual(merged_complex.id, 1)
        self.assertEqual(merged_complex.data.name, "Alice")
        self.assertEqual(merged_complex.data.age, 30)
        self.assertEqual(merged_complex.tags, ["tag1"])
        self.assertEqual(merged_complex.data.address.city, "Shanghai")
        self.assertEqual(merged_complex.data.address.zip_code, "100000")

        merged_complex = PydanticUtils.merge_object(
            source_obj=source_complex, dest_obj=dest_complex, deep_merge_config={"data": {}}
        )
        self.assertEqual(merged_complex.id, 1)
        self.assertEqual(merged_complex.data.name, "Alice")
        self.assertEqual(merged_complex.data.age, 30)
        self.assertEqual(merged_complex.tags, ["tag1"])
        self.assertEqual(merged_complex.data.address.city, "Shanghai")
        self.assertEqual(merged_complex.data.address.zip_code, None)

        # 测试类型不匹配的情况
        self.assertIsNone(PydanticUtils.merge_object(source_obj=source, dest_obj=source_complex))

        # 测试 None 值的处理
        self.assertEqual(PydanticUtils.merge_object(source_obj=None, dest_obj=dest), dest)
        self.assertEqual(PydanticUtils.merge_object(source_obj=source, dest_obj=None), source)

    def test_deserialize_basic_types(self):
        json_str = '{"id": 1, "name": "Item1"}'
        json_data = json.loads(json_str)
        self.assertEqual(Product(**json_data), Product(id=1, name="Item1"))

        json_str = '{"id": "1", "name": "Item1"}'
        json_data = json.loads(json_str)
        self.assertEqual(Product(**json_data), Product(id=1, name="Item1"))

    def test_deserialize_list_types(self):
        json_str = '{"colors": ["red", "green", "blue"]}'
        json_data = json.loads(json_str)
        self.assertEqual(Product(**json_data), Product(colors=["red", "green", "blue"]))

        json_str = '{"colors": "[\\"red\\", \\"green\\", \\"blue\\"]"}'
        json_data = json.loads(json_str)
        self.assertEqual(Product(**json_data), Product(colors=["red", "green", "blue"]))

    def test_deserialize_pydantic_types(self):
        json_str = '{"main_address": {"city": "Shanghai", "street": "Nanjing Road", "zip_code": "200000"}}'
        json_data = json.loads(json_str)
        self.assertEqual(
            Product(**json_data),
            Product(main_address=Address(city="Shanghai", street="Nanjing Road", zip_code="200000")),
        )

        json_str = '{"main_address": "{\\"city\\": \\"Shanghai\\", \\"street\\": \\"Nanjing Road\\", \\"zip_code\\": \\"200000\\"}"}'
        json_data = json.loads(json_str)
        self.assertEqual(
            Product(**json_data),
            Product(main_address=Address(city="Shanghai", street="Nanjing Road", zip_code="200000")),
        )

        json_str = '{"addresses": [{"city": "Shanghai", "street": "Nanjing Road", "zip_code": "200000"}, {"city": "Beijing", "street": "Wangfujing", "zip_code": "100000"}]}'
        json_data = json.loads(json_str)
        self.assertEqual(
            Product(**json_data),
            Product(
                addresses=[
                    Address(city="Shanghai", street="Nanjing Road", zip_code="200000"),
                    Address(city="Beijing", street="Wangfujing", zip_code="100000"),
                ]
            ),
        )

        json_str = '{"addresses": "[{\\"city\\": \\"Shanghai\\", \\"street\\": \\"Nanjing Road\\", \\"zip_code\\": \\"200000\\"}, {\\"city\\": \\"Beijing\\", \\"street\\": \\"Wangfujing\\", \\"zip_code\\": \\"100000\\"}]"}'
        json_data = json.loads(json_str)
        self.assertEqual(
            Product(**json_data),
            Product(
                addresses=[
                    Address(city="Shanghai", street="Nanjing Road", zip_code="200000"),
                    Address(city="Beijing", street="Wangfujing", zip_code="100000"),
                ]
            ),
        )


if __name__ == "__main__":
    unittest.main()
