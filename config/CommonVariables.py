from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

# from ExtractProperty import Property
from config.ExtractProperties import Property

class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str | None = None

class Settings(BaseSettings):
    ENCODING_SALT: str

    model_config = SettingsConfigDict(env_file=".env")

class User(BaseModel):
    username: str
    email: str | None = None
    full_name: str | None = None
    disabled: bool | None = None

class RegisterUser(BaseModel):
    username: str
    email: str | None = None
    full_name: str | None = None
    password: str | None = None
    hashed_password: str | None = None
    disabled: bool | None = None
    referral_code: str | None = None


class UserInDB(User):
    hashed_password: str

def get_settings():
    return Settings()

property_var = Property()