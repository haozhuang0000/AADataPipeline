from pymongo import MongoClient
import uuid
from bson.binary import UuidRepresentation
import bson


from ingestion.config import MONGODB_HOST, MONGODB_DB


def create_id(identifier_string: str):
    # if isinstance(identifier_string, list):
    #     identifier_string = " ".join(identifier_string) if identifier_string else ""
    # identifier_string = str(identifier_string)
    if not isinstance(identifier_string, str):
        identifier_string = str(identifier_string)
    _id = uuid.uuid3(uuid.NAMESPACE_DNS, identifier_string)
    _id = bson.Binary.from_uuid(
        _id, uuid_representation=UuidRepresentation.PYTHON_LEGACY
    )
    return _id


def connect_db(dbs=MONGODB_DB):

    client = MongoClient(MONGODB_HOST)
    db = client[dbs]
    return db


db = connect_db()
