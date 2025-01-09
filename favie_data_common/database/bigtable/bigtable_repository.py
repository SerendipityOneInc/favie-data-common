import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable, List, Optional, Set, Type

from google.cloud import bigtable
from google.cloud.bigtable.row import DirectRow
from google.cloud.bigtable.row_data import PartialRowData, PartialRowsData
from google.cloud.bigtable.row_filters import (
    CellsColumnLimitFilter,
    ColumnQualifierRegexFilter,
    FamilyNameRegexFilter,
    RowFilterChain,
    RowFilterUnion,
    RowKeyRegexFilter,
    TimestampRange,
    TimestampRangeFilter,
)
from google.cloud.bigtable.row_set import RowSet
from pydantic import BaseModel

from favie_data_common.common.common_utils import CommonUtils
from favie_data_common.common.pydantic_utils import PydanticUtils
from favie_data_common.database.bigtable.bigtable_utils import BigtableUtils


class FieldDeserializer:
    def deserialize(self, field_value: str):
        pass


class BigtableRepository:
    NULL_CF = "/dev/null"

    def __init__(
        self,
        *,
        bigtable_project_id: str,
        bigtable_instance_id: str,
        bigtable_table_id: str,
        model_class: Type[BaseModel],
        gen_rowkey: Callable[[BaseModel], str] = None,
        default_cf: str = None,
        cf_config: dict[str, str] = None,
        bigtable_index: "BigtableIndexRepository" = None,
        cf_migration: dict[str, (str, str)] = None,
        derializer_config: dict[str, FieldDeserializer] = None,
        model_define_deserializer: bool = False,
        charset: str = "utf-8",
    ):
        """
        bigtable_project_id: BigTable 项目 ID
        bigtable_instance_id: BigTable 实例 ID
        bigtable_table_id: BigTable 表 ID
        model_class: BigTable表对应的Pydantic模型类
        gen_row_key: 根据Pydantic模型生成row_key的函数
        cf_config: 字段对应的列族，只需要配置不对应default_cf的即可，如果为无配置则使用default_cf
        default_cf: 默认列族，如果cf_config中没有配置则使用default_cf
        bigtable_index:bigtable二级索引
        cf_migration:列族迁移配置，key为列名，value为（旧列簇,新列簇）元组
        derializer_config:字段反序列化配置，key为列名，value为FieldDeserializer对象
        """
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
        self.derializer_config = derializer_config
        # 初始化列族列表
        self.__init_cf_list()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.model_define_deserializer = model_define_deserializer
        self.logger = logging.getLogger(__name__)
        self.charset = charset

    def __init_cf_list(self):
        cf_set = set([self.default_cf]) if self.default_cf else set()  # 确保self.default_cf成为集合，即使它是字符串
        if self.cf_config:
            cf_set.update(list(self.cf_config.values()))  # 将cf_config的值用list包装来确保其作为整体加入集合，不分拆
        self.cf_list = list(cf_set)

    def save_model(
        self,
        *,
        model: BaseModel,
        save_cfs: Optional[Set[str]] = None,
        version: int = None,
        exclude_fields: list[str] = None,
        ignore_index: bool = False,
    ):
        """
        model : pydantic object need to be saved
        save_cfs : list of column families need to be saved
        version : version number of saved data
        """
        self.__save_model(model=model, save_cfs=save_cfs, version=version, exclude_fields=exclude_fields)
        if self.bigtable_index and not ignore_index:
            self.bigtable_index.save_index(model=model, version=version)

    def delete_model(self, *, model: BaseModel):
        if not model:
            return None
        if self.bigtable_index:
            self.bigtable_index.delete_index(model=model)
        self.__delete_model(row_key=self.gen_row_key(model))

    def save_models(
        self,
        *,
        models: List[BaseModel],
        save_cfs: Optional[Set[str]] = None,
        version: int = None,
        exclude_fields: list[str] = None,
        ignore_indexes: list[str] = None,
    ):
        """
        models : list of pydantic objects need to be saved
        save_cfs : list of column families need to be saved
        version : version number of saved data
        """
        self.__save_models(models=models, save_cfs=save_cfs, version=version, exclude_fields=exclude_fields)
        if self.bigtable_index:
            if ignore_indexes:
                self.bigtable_index.save_indexes(
                    models=[model for model in models if self.gen_row_key(model) not in ignore_indexes], version=version
                )
            else:
                self.bigtable_index.save_indexes(models=models, version=version)

    def delete_models(self, *, models: List[BaseModel]):
        if not models:
            return None
        if self.bigtable_index:
            self.bigtable_index.delete_indexes(models=models)
        self.__delete_models(row_keys=[self.gen_row_key(model) for model in models])

    def delete_fields(self, *, model: BaseModel, deleted_fields: list[str]):
        self.executor.submit(self.__delete_fields, self.gen_row_key(model), deleted_fields)

    def upsert_model(self, *, model: BaseModel, save_fields: list[str] = None, version: int = None):
        """
        model : pydantic object need to be upserted
        save_cf : list of column families need to be upserted
        version : version number of upserted data
        """

    def read_models(self, *, row_keys: list[str], version: int = None, fields: list[str] = None):
        """
        row_keys : list of rowkeys for data to be read
        version : version number for reading data
        fields : list of columns to read from data
        """
        if CommonUtils.list_len(row_keys) == 0:
            return None

        combined_filter = self.__gen_filters(version=version, fields=fields)
        row_set = self.__gen_row_set(row_keys)
        rows: Optional[PartialRowsData] = self.table.read_rows(row_set=row_set, filter_=combined_filter)
        if rows is None:
            self.logger.debug(f"Can't find model: row_keys = {row_keys}, version = {version},fields = {fields}")
            return None
        results = []
        for row in rows:
            results.append(self.__convert_row_to_model(row))
        return results if CommonUtils.list_len(results) > 0 else None

    def query_models(
        self, *, index_key: str, version: int = None, fields: list[str] = None, limit: int = None, filters: list = None
    ):
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
        indexes: list[BigtableIndex] = self.bigtable_index.scan_index(
            index_key=index_key, version=version, limit=limit, filters=filters
        )
        if CommonUtils.list_len(indexes) == 0:
            return None
        row_keys = [index.rowkey for index in indexes]
        results = self.read_models(row_keys=row_keys, version=version, fields=fields)
        return results

    def scan_models(
        self,
        *,
        rowkey_prefix: str,
        version: int = None,
        fields: list[str] = None,
        limit: int = None,
        filters: list = None,
    ):
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
        row_key_filter = RowKeyRegexFilter(row_key_filter_regex.encode(self.charset))
        other_filters = filters if filters else []
        other_filters.append(row_key_filter)

        combined_filter = self.__gen_filters(version=version, fields=fields, other_filters=other_filters)
        rows: Optional[PartialRowsData] = self.table.read_rows(filter_=combined_filter, limit=limit)
        if rows is None:
            self.logger.debug(f"Can't find model: row_keys = {row_keys}, version = {version},fields = {fields}")
            return None
        results = []
        for row in rows:
            results.append(self.__convert_row_to_model(row))
        return results if CommonUtils.list_len(results) > 0 else None

    def read_by_model(self, *, model: BaseModel, version: int = None, fields: list[str] = None) -> Optional[BaseModel]:
        """
        model : pydantic object need to be read
        version : version number for reading data
        fields : list of columns to read from data
        """
        row_key = self.gen_row_key(model)
        return self.read_model(row_key=row_key, version=version, fields=fields)

    def read_model(self, *, row_key: str, version: int = None, fields: list[str] = None) -> Optional[BaseModel]:
        """
        row_key : rowkey for data to be read
        version : version number for reading data
        fields : list of columns to read from data
        """
        combined_filter = self.__gen_filters(version=version, fields=fields)
        row: Optional[PartialRowData] = self.table.read_row(row_key.encode(self.charset), filter_=combined_filter)
        if row is None:
            self.logger.debug(f"Can't find model: row_key = {row_key}, version = {version} , fields = {fields}")
            return None
        return self.__convert_row_to_model(row=row, row_key=row_key)

    def __save_model(
        self,
        *,
        model: BaseModel,
        save_cfs: Optional[Set[str]] = None,
        version: int = None,
        exclude_fields: list[str] = None,
    ) -> None:
        if not model:
            return
        row = self.__convert_model_to_row(model, save_cfs=save_cfs, version=version, exclude_fields=exclude_fields)
        row.commit()

    def __delete_model(self, *, row_key: str):
        row = self.table.row(row_key.encode(self.charset))
        row.delete()
        row.commit()

    def __save_models(
        self,
        *,
        models: List[BaseModel],
        save_cfs: Optional[Set[str]] = None,
        version: int = None,
        exclude_fields: list[str] = None,
    ) -> None:
        if not models:
            return
        batcher = self.table.mutations_batcher()
        for model in models:
            row = self.__convert_model_to_row(model, save_cfs=save_cfs, version=version, exclude_fields=exclude_fields)
            batcher.mutate(row)
        batcher.flush()

    def __delete_models(self, *, row_keys: List[str]):
        if not row_keys:
            return
        batcher = self.table.mutations_batcher()
        for row_key in row_keys:
            row = self.table.row(row_key.encode(self.charset))
            row.delete()
            batcher.mutate(row)
        batcher.flush()

    def __convert_model_to_row(
        self,
        model: BaseModel,
        save_cfs: Optional[Set[str]] = None,
        version: int = None,
        exclude_fields: list[str] = None,
    ):
        """
        model : pydantic object need to be saved
        save_cfs : list of column families need to be saved
        version : version number of saved data
        """
        row_key = self.gen_row_key(model)
        row: DirectRow = self.table.direct_row(row_key.encode(self.charset))
        timestamp = None
        if version is not None:
            timestamp = datetime.fromtimestamp(version, tz=timezone.utc)
        for field_name, field_value in model.__dict__.items():
            if exclude_fields and field_name in exclude_fields:
                continue
            if self.cf_migration and field_name in self.cf_migration.keys():
                old_cf, new_cf = self.cf_migration[field_name]
                if new_cf == self.NULL_CF:
                    continue
            if field_value is None:
                continue
            column_family = (
                self.cf_config.get(field_name, self.default_cf) if self.cf_config is not None else self.default_cf
            )
            if save_cfs is None or column_family in save_cfs:
                column_value = BigtableUtils.pydantic_field_convert_str(field_value).encode(self.charset)
                if timestamp is not None:
                    row.set_cell(column_family, field_name, column_value, timestamp=timestamp)
                else:
                    row.set_cell(column_family, field_name, column_value)
        return row

    def __gen_row_set(self, row_keys: list[str]):
        row_set = RowSet()
        for row_key in row_keys:
            row_set.add_row_key(row_key.encode(self.charset))

        return row_set

    # convert bigtable row to pydantic object
    def __convert_row_to_model(self, row, row_key: str = None):
        model_dict = {}
        migration_status = set()
        for column_family in row.cells:
            for column_qualifier, cell_list in row.cells[column_family].items():
                if len(cell_list) > 0:
                    cell_value = cell_list[0].value
                    field_value = cell_value.decode(self.charset)
                    field_name = column_qualifier.decode(self.charset)
                    field_type = PydanticUtils.get_native_field_type(self.model_class, field_name)
                    # if field_type is None,ignore this field,otherwise convert it to pydantic field
                    if field_type is not None:
                        if self.cf_migration and field_name in self.cf_migration.keys():
                            old_cf, new_cf = self.cf_migration[field_name]
                            if column_family == old_cf:
                                if new_cf == self.NULL_CF:
                                    migration_status.add(field_name)
                                elif field_name not in migration_status:
                                    model_dict[field_name] = self.__derialize_field(field_name, field_value, field_type)
                            elif column_family == new_cf:
                                migration_status.add(field_name)
                                model_dict[field_name] = self.__derialize_field(field_name, field_value, field_type)
                        else:
                            model_dict[field_name] = self.__derialize_field(field_name, field_value, field_type)
        if migration_status and row_key:
            self.executor.submit(self.__delete_migeration_fields, row_key, migration_status)
        return self.model_class(**model_dict)

    def __derialize_field(self, field_name: str, field_value: str, field_type: type):
        if self.derializer_config and field_name in self.derializer_config.keys():
            return self.derializer_config[field_name].deserialize(field_value)
        if self.model_define_deserializer:
            return field_value
        else:
            return BigtableUtils.str_convert_pydantic_field(field_value, field_type)

    def __delete_migeration_fields(self, row_key: str, fields: set[str]):
        try:
            if self.cf_migration and fields:
                # row = self.table.row(row_key.encode(self.charset))
                delete_fields = []
                for field in fields:
                    if field in self.cf_migration.keys():
                        old_cf, _ = self.cf_migration[field]
                        delete_fields.append((old_cf, field))
                        # row.delete_cell(old_cf, field.encode(self.charset))
                # row.commit()
                self.__delete_fields(row_key, delete_fields)
        except Exception as e:
            self.logger.error(f"delete migration fields failed,row_key:{row_key},fields:{fields},error:{e}")

    def __delete_fields(self, row_key: str, fields: list[(str, str)]):
        try:
            if fields:
                row = self.table.row(row_key.encode(self.charset))
                for cf, field in fields:
                    row.delete_cell(cf, field.encode(self.charset))
                row.commit()
        except Exception as e:
            self.logger.error(f"delete fields failed,row_key:{row_key},fields:{fields},error:{e}")

    # generate filters for querying bigtable based on parameters
    def __gen_filters(self, *, version: Optional[str], fields: Optional[list[str]], other_filters: list = None):
        filters = [*other_filters] if other_filters else []
        # 增加版本过滤器
        if version is not None:
            start_timestamp = datetime.fromtimestamp(version, tz=timezone.utc)
            end_timestamp = datetime.fromtimestamp(version + 1, tz=timezone.utc)
            filters.append(TimestampRangeFilter(TimestampRange(start=start_timestamp, end=end_timestamp)))
        else:
            filters.append(CellsColumnLimitFilter(1))

        # 增加列过滤器
        if CommonUtils.list_len(fields) > 0:
            column_filters = [ColumnQualifierRegexFilter(qualifier.encode(self.charset)) for qualifier in fields]
            # 表达OR关系
            union_filters = (
                RowFilterUnion(filters=column_filters)
                if CommonUtils.list_len(column_filters) > 1
                else column_filters[0]
            )
            filters.append(union_filters)

        # 列簇过滤器，支持多个列簇
        families = self.__get_families(fields)
        if CommonUtils.list_len(families) > 0:
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

    # retrieve column families base on the list of fields parameters
    def __get_families(self, fields: list[str]):
        if CommonUtils.list_len(fields) == 0:
            return self.cf_list

        if self.cf_config is None:
            return self.cf_list

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
    rowkey: Optional[str] = None
    index_key: Optional[str] = None


class BigtableIndexRepository:
    def __init__(
        self,
        *,
        bigtable_project_id,
        bigtable_instance_id,
        bigtable_index_table_id,
        index_class: Type[BigtableIndex] = None,
        index_cf: str = None,
        gen_index: Callable[[BaseModel], BigtableIndex] = None,
    ):
        self.gen_index = gen_index
        self.index_table = BigtableRepository(
            bigtable_project_id=bigtable_project_id,
            bigtable_instance_id=bigtable_instance_id,
            bigtable_table_id=bigtable_index_table_id,
            model_class=index_class if index_class else BigtableIndex,
            default_cf=index_cf,
            gen_rowkey=self._gen_rowkey,
        )

    def save_index(self, *, model: BaseModel, version: int = None):
        index = self.gen_index(model)
        self.index_table.save_model(model=index, exclude_fields=["index_key"])

    def delete_index(self, *, model: BaseModel):
        index = self.gen_index(model)
        self.index_table.delete_model(model=index)

    def save_indexes(self, *, models: List[BaseModel], version: int = None):
        indexes = [self.gen_index(model) for model in models]
        self.index_table.save_models(models=indexes, exclude_fields=["index_key"])

    def delete_indexes(self, *, models: List[BaseModel]):
        indexes = [self.gen_index(model) for model in models]
        self.index_table.delete_models(models=indexes)

    def scan_index(
        self, *, index_key: str, version: int = None, filters: list = None, limit: int = None
    ) -> list[BaseModel]:
        return self.index_table.scan_models(rowkey_prefix=index_key, limit=limit, filters=filters)

    def close(self):
        if self.index_table:
            self.index_table.close()

    def _gen_rowkey(self, model: BigtableIndex):
        return f"{model.index_key}#{model.rowkey}"


class BigtableSingleMapIndexRepository(BigtableIndexRepository):
    def __init__(
        self,
        *,
        bigtable_project_id,
        bigtable_instance_id,
        bigtable_index_table_id,
        index_class: Type[BigtableIndex] = None,
        index_cf: str = None,
        gen_index: Callable[[BaseModel], BigtableIndex] = None,
    ):
        super().__init__(
            bigtable_project_id=bigtable_project_id,
            bigtable_instance_id=bigtable_instance_id,
            bigtable_index_table_id=bigtable_index_table_id,
            index_class=index_class,
            index_cf=index_cf,
            gen_index=gen_index,
        )
        self.logger = logging.getLogger(__name__)

    def scan_index(
        self, *, index_key: str, version: int = None, filters: list = None, limit: int = None
    ) -> list[BaseModel]:
        return [self.index_table.read_model(row_key=index_key)]

    def read_indexes(self, *, index_keys: List[str], version: int = None, filters: list = None):
        return self.index_table.read_models(row_keys=index_keys, version=version, fields=None)

    def _gen_rowkey(self, model):
        return model.index_key
