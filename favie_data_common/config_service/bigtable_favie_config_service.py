from favie_data_common.config_service.favie_config_service import FavieConfig, FavieConfigService
from favie_data_common.database.bigtable.bigtable_repository import BigtableRepository
from datetime import datetime

class BigtableFavieConfigService(FavieConfigService):
    default_cf = "config_cf"
    def __init__(self,*, project_id,instance_id,config_table_id,timeout_sec=60):
        super().__init__(timeout_sec)
        self.config_table_repository:BigtableRepository = BigtableRepository(
            bigtable_project_id=project_id,
            bigtable_instance_id=instance_id,
            bigtable_table_id=config_table_id,
            model_class=FavieConfig,
            default_cf=self.default_cf,
            gen_rowkey=self.config_key_generator
        )
    
    def config_key_generator(self,config_item:FavieConfig=None):
        return config_item.config_group
    
    def _is_config_updated(self,config_group)->bool:
        if not self.get_config(config_group):
            return True
        row_key = self.config_key_generator(FavieConfig(config_group=config_group))
        version_item:FavieConfig = self.config_table_repository.read_model(row_key=row_key,fields=["config_version"])
        if not version_item:
            self.logger.error(f"Config version not found: {row_key}")
            return False
        return version_item.config_version != self.get_config(config_group).config_version
    
    def _load_config(self,config_group:str)->FavieConfig:
        try:
            return  self.config_table_repository.read_model(row_key=self.config_key_generator(FavieConfig(config_group=config_group)))
        except Exception as e:
            self.logger.exception(f"Error while loading config: {e}")
    
    def upload_config(self,config_group:str,config_value:str):
        try:
            if not config_value or not config_group:
                return False
            self.config_table_repository.save_model(
                model=FavieConfig(
                    config_group=config_group,
                    config_value=config_value,
                    config_version=str(datetime.now())
                )
            )
            return True
        except Exception as e:
            self.logger.exception(f"Error while uploading config: {e}")
            return False