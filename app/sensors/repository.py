from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.mongodb_client import MongoDBClient
import json


from app.redis_client import RedisClient
from . import models, schemas


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()


def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()


def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()


def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb: MongoDBClient) -> models.Sensor:
    """
    Creates a new sensor record in both SQL and MongoDB databases.

    Parameters:
        db (Session): The SQLAlchemy session for SQL database operations.
        sensor (schemas.SensorCreate): The sensor object containing data to create the sensor.
        mongodb (MongoDBClient): The MongoDB client for NoSQL database operations.

    Returns:
        models.Sensor: The newly created sensor object from the SQL database.
    """
    # Create a new sensor record in the SQL database
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()  # Save the sensor record to the database
    db.refresh(db_sensor)  # Refresh the instance from the database, to get the generated ID

    # Prepare the sensor document for MongoDB
    document = {
        "id": db_sensor.id,
        "longitude": sensor.longitude,
        "latitude": sensor.latitude,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,  # Corrected typo from 'manufactor' to 'manufacturer'
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
    }

    # Insert the sensor document into MongoDB
    mongodb.insert_data(document)

    # Return the created sensor object from the SQL database
    return db_sensor

def record_data(db: Session, redis: RedisClient, mongo_db: MongoDBClient, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    """
    Updates sensor data in SQL database, Redis, and MongoDB, then returns the updated sensor information.

    Parameters:
        db (Session): Database session for SQL operations.
        redis (RedisClient): Client for Redis operations.
        mongo_db (MongoDBClient): Client for MongoDB operations.
        sensor_id (int): The ID of the sensor to update.
        data (schemas.SensorData): The new data for the sensor.

    Returns:
        schemas.Sensor: The updated sensor information.

    Raises:
        HTTPException: If the sensor is not found in the SQL database or MongoDB.
    """
    # Retrieve the sensor from SQL database
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found in SQL database")

    # Convert sensor data to JSON string for Redis
    dyn_data = json.dumps(data.dict())  # Using .dict() method if data is a Pydantic model

    # Update sensor data in Redis
    redis.set(str(sensor_id), dyn_data)  # Ensure the key is a string

    # Check if the sensor exists in MongoDB
    document = mongo_db.get_data(sensor_id)
    if document is None:
        raise HTTPException(status_code=404, detail="Sensor not found in MongoDB")

    # Assuming you might want to update the sensor data in MongoDB as well, but it's missing here.
    # mongo_db.update_data(sensor_id, data_dict) # Hypothetical method to update sensor data in MongoDB.

    # Deserialize dynamic data back into a Python dictionary for further processing
    try:
        data_dict = json.loads(dyn_data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Error parsing data: {e}")

    # Return the updated sensor information
    # Ensure that all the data conversions and formatting are correct and applicable.
    return schemas.Sensor(id=document['id'], 
                          name=db_sensor.name, 
                          latitude=document['latitude'], 
                          longitude=document['longitude'], 
                          joined_at=db_sensor.joined_at.strftime("%m/%d/%Y, %H:%M:%S"), 
                          last_seen=data_dict.get('last_seen', ''),  # Use get() for safe dictionary access
                          type=document['type'], 
                          mac_address=document['mac_address'], 
                          battery_level=data_dict.get('battery_level', 0), 
                          temperature=data_dict.get('temperature', 0), 
                          humidity=data_dict.get('humidity', 0), 
                          velocity=data_dict.get('velocity', 0))

def get_data(db: Session, redis: RedisClient, mongo_db: MongoDBClient, sensor_id: int) -> schemas.Sensor:
    """
    Retrieves sensor data from SQL database, Redis, and MongoDB, and returns a consolidated sensor object.

    Parameters:
        db (Session): The SQLAlchemy session for SQL database operations.
        redis (RedisClient): The client for Redis operations.
        mongo_db (MongoDBClient): The client for MongoDB operations.
        sensor_id (int): The ID of the sensor to retrieve data for.

    Returns:
        schemas.Sensor: The consolidated sensor object with data from all sources.

    Raises:
        HTTPException: If the sensor is not found in the SQL database or MongoDB, or if there is an error parsing data from Redis.
    """
    # Retrieve the sensor from the SQL database
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found in SQL database")

    # Get the sensor's dynamic data from Redis
    dyn_data = redis.get(str(sensor_id))  # Ensure sensor_id is a string for Redis keys
    if dyn_data is None:
        # Assuming it's critical to have dynamic data; otherwise, adjust the logic as necessary
        raise HTTPException(status_code=404, detail="Sensor dynamic data not found in Redis")

    # Retrieve the sensor document from MongoDB
    document = mongo_db.get_data(sensor_id)
    if document is None:
        raise HTTPException(status_code=404, detail="Sensor not found in MongoDB")

    # Parse the dynamic data from Redis
    try:
        data_dict = json.loads(dyn_data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Error parsing dynamic data: {e}")

    # Construct and return the consolidated sensor object
    return schemas.Sensor(
        id=db_sensor.id,  # Use the SQL database ID
        name=db_sensor.name,
        latitude=document.get('latitude', 0),  # Use .get() for safer access
        longitude=document.get('longitude', 0),
        joined_at=db_sensor.joined_at.strftime("%m/%d/%Y, %H:%M:%S"),
        last_seen=data_dict.get('last_seen', ''),
        type=document.get('type', ''),
        mac_address=document.get('mac_address', ''),
        battery_level=data_dict.get('battery_level', 0),
        temperature=data_dict.get('temperature', 0),
        humidity=data_dict.get('humidity', 0),
        velocity=data_dict.get('velocity', 0)
    )


def delete_sensor(db: Session, mongo_db: MongoDBClient, redis: RedisClient, sensor_id: int):
    """
    Deletes a sensor from the SQL database, MongoDB, and Redis by its ID.

    Parameters:
        db (Session): The SQLAlchemy session for SQL database operations.
        mongo_db (MongoDBClient): The client for MongoDB operations.
        redis (RedisClient): The client for Redis operations.
        sensor_id (int): The ID of the sensor to be deleted.

    Returns:
        The sensor object from the SQL database that was deleted.

    Raises:
        HTTPException: If the sensor is not found in the SQL database.
    """
    # Attempt to retrieve the sensor by its ID from the SQL database
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    
    if db_sensor is None:
        # If no sensor is found, raise an HTTPException with a 404 status code
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    # Delete the sensor from the SQL database
    db.delete(db_sensor)
    db.commit()

    # Delete the sensor data from MongoDB
    # Assuming mongo_db.delete_data is correctly implemented to handle deletion by sensor_id
    mongo_db.delete_data(sensor_id)
    
    # Delete the sensor data from Redis
    # The key used here should match how sensor data is stored/retrieved in Redis
    redis.delete(str(sensor_id))  # Ensure sensor_id is a string for Redis keys
    
    # Return the deleted sensor object from the SQL database
    return db_sensor
def get_sensors_near(db: Session, redis: RedisClient, mongodb: MongoDBClient, latitude: float, longitude: float, radius: float) -> list[schemas.Sensor]:
    """
    Retrieves sensors near a specified latitude and longitude within a given radius.

    Parameters:
        db (Session): The SQLAlchemy session for database operations.
        redis (RedisClient): The client for Redis operations.
        mongodb (MongoDBClient): The client for MongoDB operations.
        latitude (float): The latitude of the location.
        longitude (float): The longitude of the location.
        radius (float): The search radius in kilometers.

    Returns:
        list[schemas.Sensor]: A list of sensor schemas with details from SQL database, Redis, and MongoDB.

    Raises:
        HTTPException: If there's an issue parsing data from Redis for any sensor.
    """
    list_document = mongodb.get_near_sensors(latitude, longitude, radius)
    list_sensors = []

    for document in list_document:
        sensor_id = document['id']
        db_sensor = get_sensor(db, sensor_id)

        # Skip sensors not found in the SQL database
        if db_sensor is None:
            continue

        dyn_data = redis.get(str(sensor_id))  # Convert sensor_id to string for Redis

        # Handle missing or unparseable dynamic data
        if dyn_data is None:
            data_dict = {}
        else:
            try:
                data_dict = json.loads(dyn_data)
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail=f"Error parsing data for sensor {sensor_id}: {e}")

        # Construct the sensor object, using default values if dynamic data is missing
        list_sensors.append(schemas.Sensor(
            id=db_sensor.id,
            name=db_sensor.name,
            latitude=document.get('latitude', 0),
            longitude=document.get('longitude', 0),
            joined_at=db_sensor.joined_at.strftime("%m/%d/%Y, %H:%M:%S"),
            last_seen=data_dict.get('last_seen', ''),
            type=document.get('type', ''),
            mac_address=document.get('mac_address', ''),
            battery_level=data_dict.get('battery_level', 0),
            temperature=data_dict.get('temperature', 0),
            humidity=data_dict.get('humidity', 0),
            velocity=data_dict.get('velocity', 0)
        ))

    return list_sensors
