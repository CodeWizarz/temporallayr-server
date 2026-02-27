from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "Temporallayr"

    class Config:
        env_file = ".env"

settings = Settings()
