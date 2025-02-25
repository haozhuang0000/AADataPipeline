import numpy as np
from pymongo import UpdateOne, errors, InsertOne
from pymongo.collection import Collection
from typing import TypeVar, Generator
from dataclasses import asdict
from collections import OrderedDict
from ingestion.db.mongo import db
from tqdm import tqdm
from ingestion.log import Log

T = TypeVar("T")

## For NER Mapping
def upsert_record(cn: str | Collection, record: T):
    # try:
    #     collection = cn if isinstance(cn, Collection) else db[cn]
    #     ordered_dict = dict(OrderedDict(asdict(record)))
    #     collection.update_one(
    #         {"_id": record._id},
    #         {"$set": ordered_dict},
    #         upsert=True,
    #     )
    #
    #     if collection.find_one({"_id": record._id}) != asdict(record):
    #         raise Exception(f"Upsert record {record._id} failed.")
    # except Exception as e:
    #     raise Exception(f"MongoUpsert: {e}")
    try:
        collection = cn if isinstance(cn, Collection) else db[cn]
        ordered_dict = asdict(record)
        collection.insert_one(ordered_dict)

        # if collection.find_one({"_id": record._id}) != asdict(record):
        #     raise Exception(f"Upsert record {record._id} failed.")
    except errors.DuplicateKeyError:
        pass  # Ignore the error and continue execution
    except Exception as e:
        raise Exception(f"MongoUpsert: {e}")


def split_iter(list1: list[T], batch_num: int) -> Generator[list[T], None, None]:
    split_points = np.linspace(0, len(list1), batch_num + 1, dtype="uint64")
    for i in range(batch_num):
        yield list1[split_points[i] : split_points[i + 1]]


# def bulk_upsert(collection, data: list[T]):
#     if any(isinstance(i, list) for i in data):
#         data = [item for sublist in data for item in sublist]
#
#     operations = [
#         UpdateOne(
#             {"_id": rec._id},
#             {"$set": asdict(rec)},
#             upsert=True,
#         )
#         for record in data
#         for rec in (record if isinstance(record, list) else [record])
#     ]
#
#     if not isinstance(operations, list):
#         raise TypeError("bulk_write expects a list of operations")
#     if not all(isinstance(op, UpdateOne) for op in operations):
#         invalid_ops = [op for op in operations if not isinstance(op, UpdateOne)]
#         raise TypeError(f"Invalid operations found in the list: {invalid_ops}")
#
#     collection.bulk_write(operations)

# def bulk_insert(collection, data: list[T]):
#     if any(isinstance(i, list) for i in data):
#         data = [item for sublist in data for item in sublist]
#
#     operations = [
#         InsertOne(asdict(rec))
#         for record in data
#         for rec in (record if isinstance(record, list) else [record])
#     ]
#
#     if not isinstance(operations, list):
#         raise TypeError("bulk_write expects a list of operations")
#     if not all(isinstance(op, InsertOne) for op in operations):
#         invalid_ops = [op for op in operations if not isinstance(op, InsertOne)]
#         raise TypeError(f"Invalid operations found in the list: {invalid_ops}")
#
#     collection.bulk_write(operations, ordered=False)

def bulk_insert(collection, collection_name, data: list[T]):
    logger = Log("data_processing").getlog()
    if any(isinstance(i, list) for i in data):
        data = [item for sublist in data for item in sublist]

    inserted_count = 0
    skipped_duplicates = 0
    failed_count = 0

    for record in tqdm(data, desc=f"Inserting data into {collection_name}"):
        try:
            collection.insert_one(asdict(record))
            inserted_count += 1
        except errors.DuplicateKeyError:
            skipped_duplicates += 1  # Ignore and continue inserting others
        except Exception as e:
            failed_count += 1
            logger.error(f"MongoInsert Error in {collection_name}: {e}")

    logger.info(
        f"{collection_name} - Inserted: {inserted_count}, Duplicates Skipped: {skipped_duplicates}, Failed: {failed_count}")

def batch_upsert(collection_name: str | Collection, data: list[T]):
    if not isinstance(data, list):
        data = [data]
    if any(isinstance(i, list) for i in data):
        data = [item for sublist in data for item in sublist]
    if len(data) == 0:
        return

    collection = db[collection_name] if isinstance(collection_name, str) else collection_name
    # bulk_upsert(collection, data)
    bulk_insert(collection, collection_name, data)

def batch_upsert_raw(news_data: dict[str, list]):
    for key, value in news_data.items():
        value =  [v for v in value if v.Content != '' and v.Content is not None]
        batch_upsert('API_NEWS_' + key, value)