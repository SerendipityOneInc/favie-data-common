import unittest
from typing import List, Optional
from pydantic import BaseModel
from favie_data_common.common.pydantic_utils import PydanticUtils  # 请替换为实际的导入路径

class TestModel(BaseModel):
    name: Optional[str] = None
    age: Optional[int] = None

class ComplexTestModel(BaseModel):
    id: Optional[int] = None
    data: Optional[TestModel] = None
    tags: Optional[List[str]] = None

class TestPydanticUtils(unittest.TestCase):
    def test_is_type_of_list(self):
        self.assertTrue(PydanticUtils.is_type_of_list(List[int]))
        self.assertTrue(PydanticUtils.is_type_of_list(List[str]))
        self.assertFalse(PydanticUtils.is_type_of_list(int))
        self.assertFalse(PydanticUtils.is_type_of_list(str))

    def test_get_field_type(self):
        self.assertEqual(PydanticUtils.get_field_type(TestModel, "name"), str)
        self.assertEqual(PydanticUtils.get_field_type(TestModel, "age"), int)
        self.assertIsNone(PydanticUtils.get_field_type(TestModel, "non_existent_field"))

    def test_get_native_type(self):
        self.assertEqual(PydanticUtils.get_native_type(Optional[int]), int)
        self.assertEqual(PydanticUtils.get_native_type(int), int)
        self.assertEqual(PydanticUtils.get_native_type(Optional[List[str]]), List[str])

    def test_merge_object(self):
        source = TestModel(name="Alice", age=30)
        dest = TestModel(name="Bob", age=None)
        
        merged = PydanticUtils.merge_object(source_obj= source,dest_obj= dest)
        self.assertEqual(merged.name, "Alice")
        self.assertEqual(merged.age, 30)
        
        # 测试深度合并
        source_complex = ComplexTestModel(id=1, data=TestModel(name="Alice", age=30), tags=["tag1"])
        dest_complex = ComplexTestModel(id=2, data=TestModel(name="Bob", age=25), tags=["tag2"])
        
        merged_complex = PydanticUtils.merge_object(source_obj=source_complex,dest_obj= dest_complex, deep_merge_fields=["data"])
        self.assertEqual(merged_complex.id, 1)
        self.assertEqual(merged_complex.data.name, "Alice")
        self.assertEqual(merged_complex.data.age, 30)
        self.assertEqual(merged_complex.tags, ["tag1"])
        
        # 测试类型不匹配的情况
        self.assertIsNone(PydanticUtils.merge_object(source_obj=source, dest_obj=source_complex))
        
        # 测试 None 值的处理
        self.assertEqual(PydanticUtils.merge_object(source_obj=None,dest_obj= dest), dest)
        self.assertEqual(PydanticUtils.merge_object(source_obj=source,dest_obj= None), source)
        
    def test_serialize_list(self):
        test_models = [TestModel(name="jack",age=19),TestModel(name="wins",age=18)]
        self.assertEqual(PydanticUtils.serialize_list(test_models),'[{"name":"jack","age":19},{"name":"wins","age":18}]')
        
if __name__ == '__main__':
    unittest.main()