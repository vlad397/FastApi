import orjson
from pydantic import BaseModel, validator


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class BaseResponse(BaseModel):

    uuid: str

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps

    @validator('uuid')
    def id_validator(cls, value):
        return str(value)
