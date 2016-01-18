__author__ = 'azhar'
from pymongo import MongoClient


class Mongo(object):
    """ Some general purpose pymongo utilities.

    1. Insert one document.
    2. Insert many documents.
    3. Empty collection.
    4. Get distinct elements of a column.
    5. Get all documents of a collection.
    6. Get specific documents i.e., documents matching a certain condition.
    7. Remove specific documents i.e., documents matching a certain condition.

    """

    def __init__(self, host, port):
        """ Initializes the server ip and port numbers.

        Args:
            host (str): server ip address.
            port (int): server port number.

        Returns:
            MongoClient instance.

        """

        connection_string = 'mongodb://' + host + ':' + str(port) + '/'
        # TODO: Handle exceptions
        self.client = MongoClient(connection_string)

    def insert_one_doc(self, database_name, collection_name, doc):
        """ Inserts one document.

        Args:
            database_name (str): name of mongo database.
            collection_name (str): name of the collection.
            doc (dict): document to be inserted in dictionary format.
        Returns:
            None. Inserts a document into the collection.

        """

        self.client[database_name][collection_name].insert(doc)

    def insert_many_docs(self, database_name, collection_name, docs):
        """ Inserts one or more documents.

        Args:
            database_name (str): name of mongo database.
            collection_name (str): name of the collection.
            docs (list[dict]): documents to be inserted.

        Returns:
            None. Inserts document(s) into the collection.
        """

        self.client[database_name][collection_name].insert_many(docs)

    def empty_collection(self, database_name, collection_name):
        """ Removes all documents from a collection.

        Args:
            database_name (str): name of mongo database.
            collection_name (str): name of the collection.

        Returns:
            None. Removes all documents from the collection.
        """

        self.client[database_name][collection_name].delete_many({})

    def get_distinct(self, database_name, collection_name, column_name):
        """ Finds distinct elements of a given column.

        Args:
            database_name (str): name of mongo database.
            collection_name (str): name of the collection.
            column_name (str): column for which distinct elements are to be found.

        Returns:
            list[str]: Distinct elements of a column.
        """

        self.client[database_name][collection_name].distinct(column_name)

    def get_all_documents(self, database_name, collection_name):
        """ Fetches all documents from a collection. Executes the find() query.

        Args:
            database_name (str): name of mongo database.
            collection_name (str): name of the collection.

        Returns:
            pymongo cursor. An iterable within which each document is present as a dictionary.
        """

        self.client[database_name][collection_name].find()

    def get_specific_documents(self, database_name, collection_name, query):
        """ Fetches the documents satisfying the conditions mentioned in the query.

        Args:
            database_name (str): name of mongo database.
            collection_name (str): name of the collection.
            query (dict): set of conditions to be matched.
             Example: {'record_type': 'valid'}

        Returns:
            pymongo cursor. An iterable within which each document is present as a dictionary.
        """

        self.client[database_name][collection_name].find(query)

    def remove_specific_documents(self, database_name, collection_name, query):
        """ Removes the documents satisfying the conditions mentioned in the query.

        Args:
            database_name (str): name of mongo database.
            collection_name (str): name of the collection.
            query (dict): set of conditions to be matched.
             Example: {'record_type': 'invalid'}

        Returns:
            None. Removes documents if found.
        """

        self.client[database_name][collection_name].remove(query)
