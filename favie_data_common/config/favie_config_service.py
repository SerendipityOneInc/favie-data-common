import time
import threading
import logging
from typing import Optional
from pydantic import BaseModel

class FavieConfig(BaseModel):
    config_value:Optional[str] = None
    config_version:Optional[str] = None
    
class FavieConfigListener:
    def on_config_updated(self,config:FavieConfig):
        pass
    
class FavieConfigServier:
    def __init__(self, timeout_sec: int=60):
        """
        初始化配置管理类
        :param timeout_sec: 定时更新配置的时间间隔（秒）
        """
        self.timeout_sec = timeout_sec
        self.config:FavieConfig = None
        self._stop_event = threading.Event()  # 线程退出信号
        self.listener:list[FavieConfigListener] = []

        # 初始化日志记录器
        self.logger = logging.getLogger(__name__)
        
        # 启动定时更新线程
        
    def open(self):
        """
        打开配置管理器
        """
        self.__start_background_thread()
        
    def register_listener(self,listener:FavieConfigListener):
        self.listener.append(listener)

    def get_config(self) -> FavieConfig:
        """
        获取指定名称的配置项，没有时返回默认值
        :param config_name: 配置项名称
        :param default_value: 默认值
        :return: 配置项值
        """
        return self.config
    
    def _is_config_updated(self) -> bool:
        pass

    def _load_config(self)->FavieConfig:
        pass

    def __start_background_thread(self):
        """
        启动后台定时线程，用于定期更新配置
        """
        def run():
            while not self._stop_event.is_set():
                try:
                    if self._is_config_updated():
                        config = self._load_config()
                        if config:
                            self.config = config
                            for listener in self.listener:
                                listener.on_config_updated(config)
                            self.logger.info(f"Configuration updated : {config.model_dump_json()}")              
                    else:
                        self.logger.info("Configs are not update,do not need to update.")
                except Exception as e:
                    self.logger.exception("Failed to update configuration. %s", e)
                    
                time.sleep(self.timeout_sec)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self.logger.info(
            "Background thread started to refresh configuration every %s seconds", 
            self.timeout_sec
        )
        self._thread = thread

    def stop(self):
        """
        停止后台线程
        """
        self._stop_event.set()
        self.logger.info("Background thread stopped.")
