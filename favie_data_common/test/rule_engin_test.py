import datetime
from business_rules.engine import run_all
from business_rules.actions import BaseActions
from pydantic import BaseModel
from business_rules.utils import export_rule_data

# -------------------- 引入 VariablesFactory 构造动态规则变量 --------------------
from favie_data_common.rule_engine.variables_factory import VariablesFactory


class Product(BaseModel):
    """
    商品模型定义
    """
    name: str = "Default Product"  # 商品名称
    current_inventory: int = 100  # 当前库存
    expiration_date: datetime.date = datetime.date.today()  # 过期日期
    is_on_sale: bool = False  # 是否打折
    brand: str = None
    
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
    }
]


# -------------------- 测试用例 --------------------
if __name__ == "__main__":
    # 动态生成规则变量类
    ProductVariables = VariablesFactory.build_variables(Product)

    # exported_data = export_rule_data(ProductVariables, ProductActions)
    # print(exported_data)
    # exit()

    # 定义示例商品
    old_products = [
        Product(
            name="Standard Item",
            current_inventory=15,  # 旧库存
            expiration_date=datetime.date(2024, 12, 31),
            is_on_sale=False,
            brand="Brand A"
        ),
        Product(
            name="Special Promo Item",
            current_inventory=50,
            expiration_date=datetime.date(2025, 5, 30),
            is_on_sale=False  # 原来未促销
        ),
    ]
    new_products = [
        Product(
            name="Standard Item",
            current_inventory=18,  # 库存增加
            expiration_date=datetime.date(2024, 12, 31),
            is_on_sale=False
        ),
        Product(
            name="Special Promo Item",
            current_inventory=90,
            expiration_date=datetime.date(2025, 5, 30),
            is_on_sale=True,  # 新促销
            brand="Brand A"
        ),
    ]

    actions = ProductActions()

    # -------------------- 测试普通规则变量 --------------------
    print("\n--- Testing Normal Rules ---\n")
    for new_product in new_products:
        print(f"\nTesting Product: {new_product.name}")
        variables = ProductVariables(new_obj=new_product)

        # 执行规则引擎（普通字段规则）
        actions = ProductActions()
        run_all(rules, variables, actions)

    # -------------------- 测试差异规则变量 --------------------
    print("\n--- Testing Diff Rules ---\n")
    for old, new in zip(old_products, new_products):
        print(f"\nTesting Diff Product: {old.name} -> {new.name}")
        
        # 使用差异规则变量
        diff_variables = ProductVariables(new_obj=new, base_obj=old)
        
        # 执行规则引擎（差异规则）
        actions = ProductActions()
        run_all(rules, diff_variables, actions)