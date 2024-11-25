
import logging
import time
from typing import Any, Dict, Optional
from config.global_config_dev import bigtable_config
from pydantic import BaseModel

from favie_data_common.config.bigtable_favie_config_service import BigtableFavieConfigService
from favie_data_common.config.favie_config_service import FavieConfig, FavieConfigListener



class TestConfig(BaseModel):
    value:Optional[Dict[str,Any]] = None
    
class TestConfigListener(FavieConfigListener):
    def __init__(self):
        super().__init__()
        self.config:TestConfig = None
        self.logger = logging.getLogger(__name__)
        
    def on_config_updated(self,config:FavieConfig):
        try:
            self.config = TestConfig.model_validate_json(config.config_value)
            self.logger.info(f"Config updated: {self.config}")
        except Exception as e:
            self.logger.exception(f"Error while updating config: {e}")
        
    def get_config(self):
        return self.config
    
logging.basicConfig(
    level=logging.INFO,  # 设置日志级别
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # 设置日志格式
    datefmt="%Y-%m-%d %H:%M:%S"  # 设置时间格式
)

favie_config_service = BigtableFavieConfigService(
    project_id=bigtable_config["project_id"],
    instance_id=bigtable_config["instance_id"],
    config_table_id="favie_config_test",
    config_group="favie_config_test",
    timeout_sec=10
)

test_config = TestConfig(value={
        "field1":"expression1",
        "field2":"expression2",
        "field3":"expression3",
        "field4":"expression4"
    }
)

upload_result = favie_config_service.upload_config(test_config.model_dump_json())
if upload_result:
    print("upload success")

test_config_listener = TestConfigListener()
favie_config_service.register_listener(test_config_listener)
favie_config_service.start()    

for i in range(10):
    config = test_config_listener.get_config()
    if config:
        print(f"config = {config.model_dump_json()}")
    if i == 5:
        test_config.value["field5"] = "expression5"
        upload_result = favie_config_service.upload_config(test_config.model_dump_json())
    time.sleep(3)
    
favie_config_service.stop()