from favie_data_common.config.favie_config_service import FavieConfig, FavieConfigServier
from favie_data_common.database.bigtable.bigtable_repository import BigtableRepository
from datetime import datetime

class BigtableFavieConfigService(FavieConfigServier):
    default_cf = "config_cf"
    def __init__(self,*, project_id,instance_id,config_table_id,config_group,timeout_sec):
        super().__init__(timeout_sec)
        self.config_group = config_group
        self.config_table_repository:BigtableRepository = BigtableRepository(
            bigtable_project_id=project_id,
            bigtable_instance_id=instance_id,
            bigtable_table_id=config_table_id,
            model_class=FavieConfig,
            default_cf=self.default_cf,
            gen_rowkey=self.config_key_generator
        )
    
    def config_key_generator(self,config_item:FavieConfig=None):
        return self.config_group
    
    def _is_config_updated(self)->bool:
        if not self.get_config():
            return True
        version_item:FavieConfig = self.config_table_repository.read_model(row_key=self.config_key_generator(),fields=["config_version"])
        if not version_item:
            self.logger.error(f"Config version not found: {self.config_key_generator()}")
            return False
        return version_item.config_version != self.get_config().config_version
    
    def _load_config(self)->FavieConfig:
        try:
            return  self.config_table_repository.read_model(row_key=self.config_group)
        except Exception as e:
            self.logger.exception(f"Error while loading config: {e}")
    
    def upload_config(self,config:str):
        try:
            if not config:
                return False
            self.config_table_repository.save_model(model=FavieConfig(config_value=config,config_version=str(datetime.now())))
            return True
        except Exception as e:
            self.logger.exception(f"Error while uploading config: {e}")
            return False