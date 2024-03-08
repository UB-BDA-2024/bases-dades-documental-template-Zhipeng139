from pymongo import MongoClient
import os

class MongoDBClient:
    """
    A client for interacting with a MongoDB database, specifically designed for sensor data management.

    Attributes:
        host (str): The database host address.
        port (int): The database port.
        client (MongoClient): The MongoClient instance for database operations.
        database (Database): The current MongoDB database.
        collection (Collection): The current MongoDB collection.

    Methods:
        close(): Closes the MongoDB connection.
        ping(database_name="sensors"): Checks the MongoDB connection.
        setDatabase(database): Sets the current database.
        setCollection(collection): Sets the current collection.
        clearDb(database): Drops the specified database.
        insert_data(document): Inserts a document into the collection.
        get_data(sensor_id): Retrieves a document by sensor ID.
        get_near_sensors(latitude, longitude, radius): Finds sensors near a given location.
    """

    def __init__(self, host=None, port=None, db_name="sensors", collection_name="sensors_collection"):
        """
        Initializes the MongoDB client with given or default settings.

        Parameters:
            host (str): The MongoDB host. Defaults to localhost or environment variable MONGO_HOST.
            port (int): The MongoDB port. Defaults to 27017 or environment variable MONGO_PORT.
            db_name (str): The default database name. Defaults to "sensors".
            collection_name (str): The default collection name. Defaults to "sensors_collection".
        """
        self.host = host or os.getenv("MONGO_HOST", "localhost")
        self.port = port or int(os.getenv("MONGO_PORT", 27017))
        self.client = MongoClient(self.host, self.port)
        self.setDatabase(db_name)
        self.setCollection(collection_name)

    def close(self):
        """Closes the MongoDB connection."""
        self.client.close()

    def ping(self, database_name="sensors"):
        """
        Checks the MongoDB connection by pinging the specified database.

        Parameters:
            database_name (str): The name of the database to ping. Defaults to "sensors".

        Returns:
            The result of the ping command.
        """
        return self.client[database_name].command('ping')

    def setDatabase(self, database):
        """
        Sets the current database.

        Parameters:
            database (str): The name of the database to set.
        """
        self.database = self.client[database]

    def setCollection(self, collection):
        """
        Sets the current collection.

        Parameters:
            collection (str): The name of the collection to set.
        """
        self.collection = self.database[collection]

    def clearDb(self, database):
        """
        Drops the specified database.

        Parameters:
            database (str): The name of the database to drop.
        """
        self.client.drop_database(database)

    def insert_data(self, document):
        """
        Inserts a document into the collection.

        Parameters:
            document (dict): The document to insert.

        Returns:
            The result of the insert operation.
        """
        try:
            return self.collection.insert_one(document)
        except Exception as e:
            print(f"Error inserting data: {e}")
            return None

    def get_data(self, sensor_id):
        """
        Retrieves a document by sensor ID.

        Parameters:
            sensor_id (str): The ID of the sensor to find.

        Returns:
            The document with the matching sensor ID.
        """
        try:
            return self.collection.find_one({"id": sensor_id})
        except Exception as e:
            print(f"Error getting data: {e}")
            return None

    def get_near_sensors(self, latitude, longitude, radius):
        """
        Finds sensors near a given location within a specified radius.

        Parameters:
            latitude (float): The latitude of the location.
            longitude (float): The longitude of the location.
            radius (float): The radius within which to find sensors.

        Returns:
            A list of sensors within the specified radius of the given location.
        """
        try:
            return list(self.collection.find({
                "latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
                "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}
            }))
        except Exception as e:
            print(f"Error getting near sensors: {e}")
            return []

    def delete_data(self, sensor_id):
        """
        Deletes a document by sensor ID.

        Parameters:
            sensor_id (str): The ID of the sensor to delete.

        Returns:
            The result of the delete operation.
        """
        try:
            return self.collection.delete_one({"id": sensor_id})
        except Exception as e:
            return None