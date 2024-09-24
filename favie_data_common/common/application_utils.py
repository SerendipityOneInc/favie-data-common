from favie_data_common.common.common_utils import CommonUtils
from favie_data_common.database.bigtable.bigtable_utils import BigtableUtils


class ApplicationUtils:
    """
    This class contains common methods used in the application.
    """

    @staticmethod
    def get_product_detail_rowkey(url:str,sku_id:str):
        """
        Generate a rowkey for the product detail table.
        """
        domain = CommonUtils.host_trip_www(CommonUtils.get_full_subdomain(url))
        return BigtableUtils.gen_hash_rowkey(f"{sku_id}-{domain}")
    
    def get_webpage_rowkey(url:str):
        """
        Returns the current timestamp in milliseconds.
        """
        return CommonUtils.md5_hash(url)