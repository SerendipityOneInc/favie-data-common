import datetime
from typing import List, Optional
from business_rules.engine import run_all
from business_rules.actions import BaseActions
from pydantic import BaseModel
from business_rules.utils import export_rule_data
from pydantic.fields import FieldInfo

# -------------------- 引入 VariablesFactory 构造动态规则变量 --------------------
from favie_data_common.common.pydantic_utils import PydanticUtils
from favie_data_common.rule_engine.variables_factory import VariablesFactory

class Address(BaseModel):
    """
    地址模型定义
    """
    street: str
    city: str
    state: str
    zip_code: str

class Product(BaseModel):
    """
    商品模型定义
    """
    name: Optional[str] = "Default Product"  # 商品名称
    current_inventory: int = 100  # 当前库存
    expiration_date: datetime.date = datetime.date.today()  # 过期日期
    is_on_sale: bool = False  # 是否打折
    brand: Optional[str] = None
    addresses: Optional[List[Address]] = None
    main_address: Optional[Address] = None
    
    @property
    def days_to_expire(self) -> int:
        """距离过期日期的天数"""
        return (self.expiration_date - datetime.date.today()).days


# -------------------- 定义动作 --------------------
class ProductActions(BaseActions):
    """
    动作类，用于记录和执行规则触发后的操作
    """

    def __init__(self):
        self.action_results = []  # 存储操作记录

    def log_action(self, message: str):
        """
        记录触发的动作
        """
        self.action_results.append(message)
        print(f"Action Triggered: {message}")


# -------------------- 定义规则 --------------------
rules = [
    {
        # 规则 1: 如果库存低于 20
        "conditions": {
            "all": [
                {"name": "current_inventory", "operator": "less_than", "value": 20}
            ]
        },
        "actions": [{"name": "log_action", "params": {"message": "Low inventory warning!"}}]
    },
    {
        # 规则 2: 如果名称包含 “Special” 并且商品正在促销
        "conditions": {
            "all": [
                {"name": "is_on_sale", "operator": "is_true", "value": True},
                {"name": "name", "operator": "contains", "value": "Special"}
            ]
        },
        "actions": [{"name": "log_action", "params": {"message": "Promotion reminder for special item!"}}]
    },
    {
        # 规则 3: 如果距离过期日期少于 10 天
        "conditions": {
            "all": [
                {"name": "days_to_expire", "operator": "less_than", "value": 10}
            ]
        },
        "actions": [{"name": "log_action", "params": {"message": "Product is about to expire!"}}]
    },
    {
        # 规则 4: 品牌是否是 Brand A
        "conditions": {
            "all": [
                {"name": "brand", "operator": "equal_to", "value": "Brand A"}
            ]
        },
        "actions": [{"name": "log_action", "params": {"message": "Product is of brand A!"}}]
    },
    {
        # 规则 5: 比较库存差异是否大于 30
        "conditions": {
            "all": [
                {"name": "current_inventory_diff", "operator": "greater_than", "value": 30}
            ]
        },
        "actions": [{"name": "log_action", "params": {"message": "Significant inventory difference detected!"}}]
    },
    {
        # 规则 6: 测试正则匹配缓存的功能
        "conditions": {
            "all": [
                {"name": "name", "operator": "matches_cached_regex", "value": "^Special.*"}
            ]
        },
        "actions": [{"name": "log_action", "params": {"message": "Regex Matched: Special product detected!"}}]
    },
    {
        "conditions": {
            "all": [
                {"name": "addresses","operator": "contains","value": {"street": "456 Elm St", "city": "New York", "state": "NY","zip_code":"54321"}}
            ]
        },
        "actions": [{"name": "log_action",   "params": {"message": "Product contains addresses in New York!"}}]
    },
    {
        # 规则 6: 测试正则匹配缓存的功能
        "conditions": {
            "all": [
                {"name": "main_address_city", "operator": "equal_to", "value": "New York"}
            ]
        },
        "actions": [{"name": "log_action", "params": {"message": "Main addrss.city equals_to New York!"}}]
    },
    {
        "conditions": {
            "all": [
                {"name": "addresses_city","operator": "contains","value": "New York"}
            ]
        },
        "actions": [{"name": "log_action",   "params": {"message": "Product city contains addresses in New York!"}}]
    },
    
]


# -------------------- 测试用例 --------------------
if __name__ == "__main__":
    # 动态生成规则变量类

    # 定义示例商品
    old_products = [
        Product(
            name="Standard Item",
            current_inventory=15,  
            expiration_date=datetime.date(2024, 12, 31),
            is_on_sale=False,
            brand="Brand A",
            addresses=[
                Address(street="123 Main St", city="Anytown", state="NY", zip_code="12345"),
                Address(street="456 Elm St", city="New York", state="NY", zip_code="54321")
            ]
        ),
        Product(
            name="Special Promo Item",
            current_inventory=50,
            expiration_date=datetime.date(2025, 5, 30),
            is_on_sale=False,  
            addresses=[Address(street="789 Oak St", city="Sometown", state="TX", zip_code="67890")]
        ),
    ]
    new_products = [
        Product(
            name="Standard Item",
            current_inventory=18,  # 库存增加
            expiration_date=datetime.date(2024, 12, 31),
            is_on_sale=False,
            addresses=[
                Address(street="123 Main St", city="Anytown", state="NY", zip_code="12345"),
                Address(street="456 Elm St", city="New York", state="NY", zip_code="54321")
            ],
            main_address=Address(street="456 Elm St", city="New York", state="NY", zip_code="54321")
        ),
        Product(
            name="Special Promo Item",
            current_inventory=90,
            expiration_date=datetime.date(2025, 5, 30),
            is_on_sale=True,  # 新促销
            brand="Brand A",
            addresses=[Address(street="789 Oak St", city="Sometown", state="TX", zip_code="67890")]
        ),
    ]

    actions = ProductActions()

    # -------------------- 测试普通规则变量 --------------------
    print("\n--- Testing Normal Rules ---\n")
    ProductVariables = VariablesFactory.build_variables(Product)
    # PydanticUtils.get_fields_of_pydantic_class(Address)
    
    # fields: dict[str, FieldInfo] = Address.model_fields
    # for field_name, field_info in fields.items():
    #     print(f"{field_name}: {type(field_info)}")
    for new_product in new_products:
        print(f"\nTesting Product: {new_product.name}")
        variables = ProductVariables(new_obj=new_product)
        # print(f"addresses = {variables.addresses}")
        # print(f"addresses() = {variables.addresses()}")
        # exit(0)
        # 执行规则引擎（普通字段规则）
        actions = ProductActions()
        # print(export_rule_data(ProductVariables, ProductActions))
        run_all(rules, variables, actions)

    # -------------------- 测试差异规则变量 --------------------
    # print("\n--- Testing Diff Rules ---\n")
    # for old, new in zip(old_products, new_products):
    #     print(f"\nTesting Diff Product: {old.name} -> {new.name}")
        
    #     # 使用差异规则变量
    #     diff_variables = ProductVariables(new_obj=new, base_obj=old)
        
    #     # 执行规则引擎（差异规则）
    #     actions = ProductActions()
    #     run_all(rules, diff_variables, actions)
        
        