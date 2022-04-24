from typing import ClassVar, Optional

from models.base import BaseMovie


class Film(BaseMovie):
    '''Полная модель фильма'''
    index: ClassVar[str] = 'movies'

    title: str
    imdb_rating: float
    description: str
    genre: list
    actors: Optional[list]
    writers: Optional[list]
    directors: Optional[list]
    actors_names: Optional[list]
    writers_names: Optional[list]
    directors_names: Optional[list]
