from business_rules.engine import run_all
from business_rules.variables import BaseVariables
from business_rules.actions import BaseActions
import re


# 定义变量（用于提供爬取数据）
class ScrapedDataVariables(BaseVariables):
    def __init__(self, current_data, previous_data):
        self.current_data = current_data
        self.previous_data = previous_data

    def field_value_equals(self, field_name, value):
        return self.current_data.get(field_name) == value

    def field_greater_than(self, field_name, value):
        return self.current_data.get(field_name, 0) > value

    def field_changes(self, field_name):
        return self.current_data.get(field_name) != self.previous_data.get(field_name)

    def list_size_differs(self, field_name, diff):
        current_list = self.current_data.get(field_name, [])
        previous_list = self.previous_data.get(field_name, [])
        return len(current_list) - len(previous_list) == diff

    def matches_regex(self, field_name, pattern):
        value = self.current_data.get(field_name, "")
        return bool(re.match(pattern, value))


# 定义规则触发的动作（用于打印或存储触发的结果）
class ScrapedDataActions(BaseActions):
    def __init__(self):
        self.actions_triggered = []

    def add_to_triggered_actions(self, action_message):
        self.actions_triggered.append(action_message)
        print(f"Action triggered: {action_message}")


# 创建规则（JSON 格式）
rules = [
    {
        "conditions": {
            "all": [
                {"name": "field_value_equals", "params": {"field_name": "status", "value": "error"}},
                {"name": "matches_regex", "params": {"field_name": "title", "pattern": r"^Critical.*"}}
            ]
        },
        "actions": [
            {"name": "add_to_triggered_actions", "params": {"action_message": "Critical error detected!"}}
        ]
    },
    {
        "conditions": {
            "any": [
                {"name": "field_greater_than", "params": {"field_name": "price", "value": 5000}},
                {"name": "field_changes", "params": {"field_name": "price"}}
            ]
        },
        "actions": [
            {"name": "add_to_triggered_actions", "params": {"action_message": "Price has changed or exceeded limit!"}}
        ]
    },
    {
        "conditions": {
            "all": [
                {"name": "list_size_differs", "params": {"field_name": "availabilities", "diff": 2}}
            ]
        },
        "actions": [
            {"name": "add_to_triggered_actions", "params": {"action_message": "List size differs by 2!"}}
        ]
    }
]

# 示例如下：
current_data = {
    "status": "error",
    "title": "Critical System Failure",
    "price": 5500,
    "availabilities": ["store_1", "store_2", "store_3"]
}

previous_data = {
    "status": "ok",
    "title": "System Running Smoothly",
    "price": 5200,
    "availabilities": ["store_1"]
}


# 运行规则引擎
variables = ScrapedDataVariables(current_data=current_data, previous_data=previous_data)
actions = ScrapedDataActions()

# 执行规则
run_all(rules, variables, actions)

print("Actions Triggered:", actions.actions_triggered)
