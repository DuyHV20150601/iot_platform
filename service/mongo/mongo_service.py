import pymongo
from utils.logger import Logger

logger = Logger(__name__).logger


class MongoService(object):
    def __init__(self, host, port, log_obj):
        self.logger = log_obj.logger
        self.__host = host
        self.__port = port

        try:
            self.mongo_client = pymongo.MongoClient('mongodb://%s:%s/' % (self.__host, self.__port))

        except Exception as e:
            self.logger.error('Cannot connect to mongos, [ERROR: %s]' % e)

    def get_db(self, db_name):
        return self.mongo_client[db_name]

    def get_collection(self, collection_name, mongo_database):
        return self.get_db(db_name=mongo_database)[collection_name]

    def insert(self, list_of_message, collection):

        try:
            self.logger.info('Insert to [COLLECTION: %s], [MESSAGE: %s]' % (collection, list_of_message))
            collection.insert_many(list_of_message)

        except Exception as e:
            self.logger.error('Error occurs: [ERROR: %s]' % e)

    def insert_one(self, message, collection):

        try:
            collection.insert_one(message)

        except Exception as e:
            self.logger.error('Insert false, [ERROR: %s]' % e)


if __name__ == '__main__':
    mongo = MongoService(host='127.0.0.1', port=27017)
    iot_platform = mongo.get_db('iot_platform')
    print('Create db: iot_platform')
    collection = mongo.get_collection('test', iot_platform)
    print('collection')
    my_dict = [
        {"name": "John", "address": "Highway 37"},
        {"name": "Duy", "address": "test"},
        {"name": "Tests", "address": "Test"}
    ]
    collection.insert_many(my_dict)
