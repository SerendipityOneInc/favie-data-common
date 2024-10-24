from datetime import datetime,timezone
import logging
from typing import Type, Set, Callable,Optional, get_args
from pydantic import BaseModel
from google.cloud import bigtable
from google.cloud.bigtable.row import DirectRow
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_data import PartialRowData,PartialRowsData
from google.cloud.bigtable.row_filters import (
    TimestampRange, 
    TimestampRangeFilter,
    ColumnQualifierRegexFilter,
    CellsColumnLimitFilter,
    RowFilterChain,
    RowFilterUnion,
    FamilyNameRegexFilter,
    RowKeyRegexFilter
)
from favie_data_common.database.bigtable.bigtable_utils import BigtableUtils
from favie_data_common.common.common_utils import CommonUtils
from favie_data_common.common.pydantic_utils import PydanticUtils
from concurrent.futures import ThreadPoolExecutor

class BigtableRepository:
    def __init__(self,*,
                 bigtable_project_id:str,
                 bigtable_instance_id:str,
                 bigtable_table_id:str,
                 model_class:Type[BaseModel],
                 gen_rowkey:Callable[[BaseModel], str] = None,
                 default_cf:str = None,
                 cf_config:dict[str,str]=None,
                 bigtable_index:'BigtableIndexRepository'=None,
                 cf_migration:dict[str,(str,str)]=None
            ):
        '''
            bigtable_project_id: BigTable 项目 ID
            bigtable_instance_id: BigTable 实例 ID
            bigtable_table_id: BigTable 表 ID
            model_class: BigTable表对应的Pydantic模型类
            gen_row_key: 根据Pydantic模型生成row_key的函数
            cf_config: 字段对应的列族，只需要配置不对应default_cf的即可，如果为无配置则使用default_cf
            default_cf: 默认列族，如果cf_config中没有配置则使用default_cf
            bigtable_index:bigtable二级索引
            cf_migration:列族迁移配置，key为列名，value为（旧列簇,新列簇）元组
        '''
        self.bigtable_project_id = bigtable_project_id
        self.bigtable_instance_id = bigtable_instance_id
        self.bigtable_table_id = bigtable_table_id
        self.client = bigtable.Client(self.bigtable_project_id)
        self.instance = self.client.instance(self.bigtable_instance_id)
        self.table = self.instance.table(self.bigtable_table_id)
        self.model_class = model_class
        self.gen_row_key = gen_rowkey
        self.cf_config = cf_config
        self.default_cf = default_cf
        self.bigtable_index = bigtable_index
        self.cf_migration = cf_migration
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.logger = logging.getLogger(__name__)
    
    def save_model(self, * ,model: BaseModel,save_cfs:Optional[Set[str]] = None,version:int = None,exclude_fields:list[str]=None):
        """
            model : pydantic object need to be saved
            save_cfs : list of column families need to be saved
            version : version number of saved data
        """
        self.__save_model(model=model,save_cfs=save_cfs,version=version,exclude_fields=exclude_fields)
        if self.bigtable_index:
            self.bigtable_index.save_index(model = model,version=version)        
        
    def upsert_model(self,*,model: BaseModel,save_fields:list[str] = None,version:int = None):
        """
            model : pydantic object need to be upserted
            save_cf : list of column families need to be upserted
            version : version number of upserted data
        """
        pass
        
    def read_models(self,*,row_keys:list[str],version:int = None,fields:list[str] = None):
        """
            row_keys : list of rowkeys for data to be read
            version : version number for reading data
            fields : list of columns to read from data
        """
        if CommonUtils.list_len(row_keys) == 0:
            return None
        
        combined_filter = self.__gen_filters(version=version,fields=fields)
        row_set = self.__gen_row_set(row_keys)
        rows: Optional[PartialRowsData]  = self.table.read_rows(row_set=row_set,filter_=combined_filter) 
        if rows is None:
            self.logger.debug(f"Can't find model: row_keys = {row_keys}, version = {version},fields = {fields}")
            return None
        results = []
        for row in rows:
            results.append(self.__convert_row_to_model(row))
        return results if CommonUtils.list_len(results) > 0 else None
    
    def query_models(self,*,index_key:str,version:int = None,fields:list[str] = None,limit:int=None,filters:list=None):
        """
            Query Bigtable by index key.
            
            Args:
            index_key : query by index key 
            version : version number for reading data
            fields : list of columns to read from data
        """
        if not self.bigtable_index:
            self.logger.error("Bigtable index is not configured")
            return None
        start = datetime.now().timestamp()
        indexes: list[BigtableIndex] = self.bigtable_index.scan_index(index_key=index_key,version=version,limit=limit,filters=filters)
        if CommonUtils.list_len(indexes) == 0:
            return None
        self.logger.info("Query index time cost: %s",datetime.now().timestamp()-start)
        
        start = datetime.now().timestamp()
        row_keys = [index.rowkey for index in indexes]
        results = self.read_models(row_keys=row_keys,version=version,fields=fields)
        self.logger.info("Query data time cost: %s",datetime.now().timestamp()-start)
        return results
    
    def scan_models(self,*,rowkey_prefix:str,version:int = None,fields:list[str] = None,limit:int=None,filters:list=None):
        """
            Scan Bigtable rowkeys based on a given prefix.
            
            Args:
            rowkey_prefix : the prefix to scan for in rowkeys
            version : version number for reading data
            fields : list of columns to read from data
        """
        if not rowkey_prefix:
            return None
        
        row_key_filter_regex = f"{rowkey_prefix}.*"
        row_key_filter = RowKeyRegexFilter(row_key_filter_regex.encode('utf-8'))
        other_filters = filters if filters else []
        other_filters.append(row_key_filter)

        combined_filter = self.__gen_filters(version=version,fields=fields,other_filters=other_filters)
        rows: Optional[PartialRowsData]  = self.table.read_rows(filter_=combined_filter,limit=limit) 
        if rows is None:
            self.logger.debug(f"Can't find model: row_keys = {row_keys}, version = {version},fields = {fields}")
            return None
        results = []
        for row in rows:
            results.append(self.__convert_row_to_model(row))
        return results if CommonUtils.list_len(results) > 0 else None
    
    def read_model(self, *,row_key: str,version:int = None,fields:list[str] = None) -> Optional[BaseModel]:
        """
            row_key : rowkey for data to be read
            version : version number for reading data
            fields : list of columns to read from data
        """
        combined_filter = self.__gen_filters(version=version,fields=fields)
        row: Optional[PartialRowData]  = self.table.read_row(row_key,filter_=combined_filter) 
        if row is None:
            self.logger.debug(f"Can't find model: row_key = {row_key}, version = {version} , fields = {fields}")
            return None
        return self.__convert_row_to_model(row=row,row_key=row_key)
    
    def __save_model(self, * ,model: BaseModel,save_cfs:Optional[Set[str]] = None,version:int = None,exclude_fields:list[str]=None) -> None:
        """
            model : pydantic object need to be saved
            save_cfs : list of column families need to be saved
            version : version number of saved data
        """
        row_key = self.gen_row_key(model)
        row: DirectRow = self.table.direct_row(row_key)
        timestamp = None
        if version is not None:
            timestamp = datetime.fromtimestamp(version, tz=timezone.utc)
        for field_name, field_value in model.__dict__.items():
            if exclude_fields and field_name in exclude_fields:
                continue
            if field_value is None:
                continue
            column_family = self.cf_config.get(field_name,self.default_cf) if self.cf_config is not None else self.default_cf
            if save_cfs is None or  column_family in save_cfs:
                column_value = BigtableUtils.pydantic_field_convert_str(field_value).encode('utf-8')
                if timestamp is not None:
                    row.set_cell(column_family, field_name, column_value,timestamp=timestamp)
                else:
                    row.set_cell(column_family, field_name, column_value)
        row.commit()
        
    
    def __gen_row_set(self,row_keys:list[str]):
        row_set = RowSet()
        for row_key in row_keys:
            row_set.add_row_key(row_key)
            
        return row_set

    #convert bigtable row to pydantic object
    def __convert_row_to_model(self,row,row_key:str=None):
        model_dict = {}
        migration_status = set()
        for column_family in row.cells:
            for column_qualifier, cell_list in row.cells[column_family].items():
                if len(cell_list) > 0:
                    cell_value = cell_list[0].value
                    field_value = cell_value.decode('utf-8')
                    field_name = column_qualifier.decode('utf-8')
                    field_type = PydanticUtils.get_field_type(self.model_class, field_name)
                    #if field_type is None,ignore this field,otherwise convert it to pydantic field
                    if field_type is not None:
                        if self.cf_migration and field_name in self.cf_migration.keys():
                            old_cf,new_cf = self.cf_migration[field_name]
                            if column_family == old_cf and field_name not in migration_status:
                                model_dict[field_name] = BigtableUtils.str_convert_pydantic_field(field_value, field_type)
                            elif column_family == new_cf:
                                migration_status.add(field_name)
                                model_dict[field_name] = BigtableUtils.str_convert_pydantic_field(field_value, field_type)
                        else:
                            model_dict[field_name] = BigtableUtils.str_convert_pydantic_field(field_value, field_type)
        if migration_status and row_key:
            self.executor.submit(self.__delete_migeration_fields,row_key,migration_status)
        return self.model_class(**model_dict)
    
    
    def __delete_migeration_fields(self,row_key:str,fields:set[str]):
        try:
            if self.cf_migration and fields:
                row = self.table.row(row_key)
                for field in fields:
                    if field in self.cf_migration.keys():
                        old_cf,new_cf = self.cf_migration[field]
                        row.delete_cell(old_cf,field.encode("utf-8"))
                row.commit()
        except Exception as e:
            self.logger.error(f"delete migration fields failed,row_key:{row_key},fields:{fields},error:{e}")


    #generate filters for querying bigtable based on parameters
    def __gen_filters(self,*,version:Optional[str],fields:Optional[list[str]],other_filters:list = None):
        filters = [*other_filters] if other_filters else []
        #增加版本过滤器
        if version is not None:
            start_timestamp = datetime.fromtimestamp(version, tz=timezone.utc)
            end_timestamp = datetime.fromtimestamp(version + 1, tz=timezone.utc)
            filters.append(TimestampRangeFilter(TimestampRange(start=start_timestamp, end=end_timestamp)))
        else:
            filters.append(CellsColumnLimitFilter(1))

        #增加列过滤器
        if CommonUtils.list_len(fields) > 0:
            column_filters =  [ColumnQualifierRegexFilter(qualifier.encode('utf-8')) for qualifier in fields]
            #表达OR关系
            union_filters =  RowFilterUnion(filters=column_filters) if CommonUtils.list_len(column_filters) > 1 else column_filters[0]
            filters.append(union_filters)

        # 列簇过滤器，支持多个列簇
        families = self.__get_families(fields)
        if CommonUtils.list_len(families)>0:
            family_filters = [FamilyNameRegexFilter(family) for family in families]
            if CommonUtils.list_len(family_filters) > 1:
                family_filter_union = RowFilterUnion(filters=family_filters)
                filters.append(family_filter_union)
            elif CommonUtils.list_len(family_filters) == 1:
                filters.append(family_filters[0])

        # 根据 filters 列表的长度决定如何调用 read_row
        combined_filter = None
        if len(filters) > 1:
            combined_filter = RowFilterChain(filters=filters)
        elif len(filters) == 1:
            combined_filter = filters[0]
            
        return combined_filter

    
    #retrieve column families base on the list of fields parameters
    def __get_families(self,fields:list[str]):
        if CommonUtils.list_len(fields) == 0:
            return None
        if self.cf_config is None:
            return None
        families = set()
        for field in fields:
            if field in self.cf_config.keys():
                families.add(self.cf_config[field])
            else:
                families.add(self.default_cf)
        return families
    
    def close(self):
        self.client.close()
        self.executor.shutdown()
        
        
class BigtableIndex(BaseModel):
    rowkey:Optional[str] = None
    index_key:Optional[str] = None
        
class BigtableIndexRepository:
    def __init__(self,*,
                 bigtable_project_id,
                 bigtable_instance_id,
                 bigtable_index_table_id,
                 index_class: Type[BigtableIndex] = None,    
                 index_cf:str = None,
                 gen_index: Callable[[BaseModel], BigtableIndex] = None
            ):
        self.gen_index = gen_index
        self.index_table = BigtableRepository(
            bigtable_project_id=bigtable_project_id,
            bigtable_instance_id=bigtable_instance_id,
            bigtable_table_id=bigtable_index_table_id,
            model_class=index_class if index_class else BigtableIndex,
            default_cf=index_cf,
            gen_rowkey=self.__gen_rowkey
        )
     
    def save_index(self,*,model:BaseModel,version:int = None):
        index = self.gen_index(model)
        self.index_table.save_model(model=index,exclude_fields=["index_key"])

    def scan_index(self,*,index_key:str,version:int=None,filters:list=None,limit:int=None)->list[BaseModel]:
        models = self.index_table.scan_models(rowkey_prefix=index_key,limit=limit,filters=filters)
        return models
        
    def __gen_rowkey(self,model:BigtableIndex):
        return f'{model.index_key}#{model.rowkey}'