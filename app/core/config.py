from pydantic import BaseSettings
import os

class Settings(BaseSettings):
    PROJECT_NAME: str = "Ticker Service"
    API_V1_STR: str = "/api/v1"
    postgres_user:str
    postgres_password:str
    postgres_host:str
    postgres_port:int
    postgres_database:str
    postgres_host:str
    rabbitmq_url:str
    rabbitmq_host:str
    rabbitmq_port:int
    rabbitmq_user:str
    rabbitmq_password:str
    redis_host:str
    redis_port:int
    mongo_uri:str
    mongo_user:str
    mongo_password:str
    mongo_host:str
    mongo_port:int
    mongo_database:str
    BROKER_NAME: str
    USER_NAME: str


    class Config:
        case_sensitive = True
        env_file = ".env"  # Load environment variables from .env file

