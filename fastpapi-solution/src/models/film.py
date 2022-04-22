from typing import Optional

import orjson
from pydantic import BaseModel


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class Film_Detail(BaseModel):
    '''Модель для одного фильма'''
    uuid: str
    title: str
    imdb_rating: float
    description: str
    genre: list
    actors: Optional[list]
    writers: Optional[list]
    directors: Optional[list]

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps


class Films(BaseModel):
    '''Модель для списка фильмов'''
    uuid: str
    title: str
    imdb_rating: Optional[float]

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps

