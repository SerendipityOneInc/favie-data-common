import json
from typing import List, Optional

import requests
from pydantic import BaseModel


class HttpClient:
    @staticmethod
    def call_http_api(url, result_class: BaseModel, method="POST", headers=None, data=None, params=None):
        try:
            # 根据请求方法选择合适的requests方法
            method = method.upper()

            if headers is None:
                headers = {"Content-Type": "application/json"}

            # 如果是 JSON 数据，确保序列化为 JSON 格式
            if isinstance(data, dict):
                data = json.dumps(data)

            if method == "POST":
                response = requests.post(url, headers=headers, data=data, params=params)
            elif method == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif method == "PUT":
                response = requests.put(url, headers=headers, data=data, params=params)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # 检查是否成功响应
            response.raise_for_status()  # 如果返回错误状态码，将引发HTTPError

            return result_class.model_validate(response.json())  # 返回 JSON 格式的响应

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error: {http_err}")
        except Exception as err:
            print(f"Error: {err}")


class CategoryPredictResult(BaseModel):
    ids: Optional[List[List[str]]] = None
    categories: Optional[List[List[str]]] = None
    scores: Optional[List[List[float]]] = None


if __name__ == "__main__":
    # 示例：调用上面提到的 NLP API
    url = "http://favie-nlp-service.favie.svc.cluster.local/nlp/category_predict"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    data = {
        "type": "product_domain",
        "model_id": "category_prediction",
        "inputs": ["string"],
        "topk": 5,
        "return_score": True,
    }

    response = HttpClient.call_http_api(url, CategoryPredictResult, method="POST", headers=headers, data=data)
    print(response.model_dump_json())
