from pymongo import MongoClient
from contextlib import contextmanager
import conf.mongo as db_config
from src.lib.log import Logger

log = Logger()


class MongoDBManager:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(MongoDBManager, cls).__new__(cls)
            cls._instance.client = MongoClient(db_config.host, db_config.port)
            cls._instance.db_name = db_config.db_name
        return cls._instance

    def __del__(self):
        self.client.close()

    def __validate_collection_name(self, collection_name):
        if not collection_name or not isinstance(collection_name, str):
            raise ValueError("Collection name must be a non-empty string.")

    @contextmanager
    def connect(self):
        try:
            db = self.client[self.db_name]
            yield db
        except Exception as e:
            log.logger.error(f"Database connection error: {e}")
            raise

    def insert_one(self, collection_name, data):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                result = collection.insert_one(data)
            return result.inserted_id
        except Exception as e:
            log.logger.error(f"insert_one operation failed: {e}")
            raise

    def insert_many(self, collection_name, data_list):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                result = collection.insert_many(data_list)
            return result.inserted_ids
        except Exception as e:
            log.logger.error(f"insert_many operation failed: {e}")
            raise

    def find_max(self, collection_name, query, sort=[("_id", -1)]):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                return collection.find(query).sort(sort).limit(1)
        except Exception as e:
            log.logger.error(f"find_max operation failed: {e}")
            raise

    def find_one(self, collection_name, query):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                res = list(collection.find(query))
                if res is None:
                    return
                if len(res) > 0:
                    return res[-1]
        except Exception as e:
            log.logger.error(f"find_one operation failed: {e}")
            raise

    def find_list(self, collection_name, query):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                return list(collection.find(query))
        except Exception as e:
            log.logger.error(f"find_list operation failed: {e}")
            raise

    def counts(self, collection_name, query):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                return collection.count_documents(query)
        except Exception as e:
            log.logger.error(f"counts Find operation failed: {e}")
            raise

    def update_one(self, collection_name, query, update):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                result = collection.update_one(query, {"$set": update})
            return result.modified_count
        except Exception as e:
            log.logger.error(f"update_one operation failed: {e}")
            raise

    def update_many(self, collection_name, query, update):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                result = collection.update_many(query, {"$set": update})
            return result.modified_count
        except Exception as e:
            log.logger.error(f"update_many operation failed: {e}")
            raise

    def delete_one(self, collection_name, query):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                result = collection.delete_one(query)
            return result.deleted_count
        except Exception as e:
            log.logger.error(f"delete_one operation failed: {e}")
            raise

    def delete_many(self, collection_name, query):
        self.__validate_collection_name(collection_name)
        try:
            with self.connect() as db:
                collection = db[collection_name]
                result = collection.delete_many(query)
            return result.deleted_count
        except Exception as e:
            log.logger.error(f"delete_many operation failed: {e}")
            raise
