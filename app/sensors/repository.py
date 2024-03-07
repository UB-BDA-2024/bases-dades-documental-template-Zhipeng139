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
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    document = {
        "id": db_sensor.id,
        "longitude": sensor.longitude,
        "latitude": sensor.latitude,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufactor": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
    }
    mongodb.insert_data(document)
    return db_sensor


def record_data(db: Session, redis: RedisClient, mongo_db: MongoDBClient, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(
            status_code=404, detail=f"Sensor with id: {sensor_id} are not in db")

    dyn_data = json.dumps(data.__dict__)  # convert to dict

    redis.set(sensor_id, dyn_data)

    document = mongo_db.get_data(sensor_id)
    if document is None:
        raise HTTPException(
            status_code=404, detail=f"Sensor with id: {sensor_id} are not in mongo")

    try:
        data_dict = json.loads(dyn_data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Error parsing data: {e}")

    return schemas.Sensor(id=document['id'], 
                          name=db_sensor.name, 
                          latitude=document['latitude'], 
                          longitude=document['longitude'], 
                          joined_at=db_sensor.joined_at.strftime("%m/%d/%Y, %H:%M:%S"), 
                          last_seen=data_dict['last_seen'], 
                          type=document['type'], 
                          mac_address=document['mac_address'], 
                          battery_level=data_dict['battery_level'], 
                          temperature=data_dict['temperature'], 
                          humidity=data_dict['humidity'], 
                          velocity=data_dict['velocity'])


def get_data(db: Session, redis: RedisClient, mongo_db: MongoDBClient, sensor_id: int) -> schemas.Sensor:
    
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(
            status_code=404, detail=f"Sensor with id: {sensor_id} are not in db")

    dyn_data = redis.get(sensor_id)
    document = mongo_db.get_data(sensor_id)

    if document is None:
        raise HTTPException(
            status_code=404, detail=f"Sensor with id: {sensor_id} are not in mongo")

    try:
        data_dict = json.loads(dyn_data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Error parsing data: {e}")

    return schemas.Sensor(id=document['id'], 
                          name=db_sensor.name, 
                          latitude=document['latitude'], 
                          longitude=document['longitude'], 
                          joined_at=db_sensor.joined_at.strftime("%m/%d/%Y, %H:%M:%S"), 
                          last_seen=data_dict['last_seen'], 
                          type=document['type'], 
                          mac_address=document['mac_address'], 
                          battery_level=data_dict['battery_level'], 
                          temperature=data_dict['temperature'], 
                          humidity=data_dict['humidity'], 
                          velocity=data_dict['velocity'])


def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(
        models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor
