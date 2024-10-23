import time
from typing import Optional
from pydantic import BaseModel
from favie_data_common.database.bigtable.bigtable_repository import BigtableRepository,BigtableIndexRepository,BigtableIndex
from favie_data_common.common.common_utils import CommonUtils
import google.cloud.bigtable.row_filters as sync_row_filters
from google.cloud.bigtable import Client

bigtable_config = {
    "project_id": "srpdev-7b1d3",
    "instance_id": "favie-product-merge-db",
}

class Person(BaseModel):
    id : Optional[str] = None
    name : Optional[str] = None
    age : Optional[int] = None
    city : Optional[str] = None
    sex : Optional[str] = None
    address : Optional[str] = None
    favorite : Optional[str] = None

def gen_review_rowkey(person:Person):
    return person.id

def gen_review_index(person:Person):
    return BigtableIndex(
        rowkey=gen_review_rowkey(person=person),
        index_key=person.city
    )

person_city_index_repository = BigtableIndexRepository(
    bigtable_project_id=bigtable_config["project_id"],
    bigtable_instance_id=bigtable_config["instance_id"],
    bigtable_index_table_id="favie_test_table_index",
    index_class=BigtableIndex,
    index_cf="index_cf",
    gen_index=gen_review_index
)
cf_migeration: dict[str, tuple[str, str]] = {
    "address":("main_cf","new_cf"),
    "city":("main_cf","new_cf")
}

cf_config={
    "address":"new_cf",
    "city":"new_cf"
}


person_repository = BigtableRepository(
    bigtable_project_id=bigtable_config["project_id"],
    bigtable_instance_id=bigtable_config["instance_id"],
    bigtable_table_id="favie_test_table",
    model_class=Person,
    gen_rowkey=gen_review_rowkey,
    default_cf="main_cf",
    cf_config=cf_config,
    bigtable_index=person_city_index_repository,
    cf_migration=cf_migeration    
)

def test_save():
    for i in range(1,10):
        person = Person(
            id=f"B0000{i}",
            name=f"Bob{i}",
            sex="male",
            address=f"address{i}",
            favorite=f"favorite{i}",
            city="hangzhou" if i % 2 == 0 else "beijing"
        )
        person_repository.save_model(model=person)


def test_scan():
    persons = person_repository.scan_models(rowkey_prefix="B",fields=["name","sex"],limit=3)
    if CommonUtils.not_empty(persons):
        for person in persons:
            print(person.model_dump_json(exclude_none=True))
 
def test_query_by_city():
    reviews = person_repository.query_models(index_key="hangzhou")
    if CommonUtils.not_empty(reviews):
        for review in reviews:
            print(review.model_dump_json(exclude_none=True))
            
def test_read_with_cf_migeration():
    for i in range(1,10):
        person = person_repository.read_model(row_key=f"B0000{i}")
        print(person.model_dump_json(exclude_none=True))
                
if __name__ == "__main__":
    test_save()
    # test_scan()
    # test_query_by_city()
    test_read_with_cf_migeration()
    time.sleep(10)