import uuid
from typing import ClassVar

import orjson
from pydantic import BaseModel, validator


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class BaseMovie(BaseModel):
    index: ClassVar[str] = None

    id: uuid.UUID

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps

    @validator('id')
    def id_validator(cls, value):
        return str(value)
