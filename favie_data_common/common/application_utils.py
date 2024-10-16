from typing import Optional
from favie_data_common.common.common_utils import CommonUtils
from favie_data_common.database.bigtable.bigtable_utils import BigtableUtils


class ApplicationUtils:
    """
    This class contains common methods used in the application.
    """

    @staticmethod
    def get_product_detail_rowkey(url:str,sku_id:str):
        """
        Generate a rowkey for a product detail.
        """
        domain = CommonUtils.host_trip_www(CommonUtils.get_full_subdomain(url))
        return BigtableUtils.gen_hash_rowkey(f"{sku_id}-{domain}")
    
    @staticmethod
    def get_webpage_rowkey(url:str):
        """
        Generate a rowkey for a webpage table.
        """
        return CommonUtils.md5_hash(url)
    
    @staticmethod
    def get_sku_id_info(f_sku_id:str) -> Optional[tuple[str, str]]:
        """
        Return (sku_id,site) parsed from f_sku_id .
        """
        if not f_sku_id:
            return None
        
        items = f_sku_id.split("-")
        if len(items) != 2:
            return None
        
        return (items[0], items[1])