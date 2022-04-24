from typing import List, Optional, ClassVar

from models.base import BaseMovie


class Film(BaseMovie):
    """Модель для одного фильма"""

    index: ClassVar[str] = 'movies'

    uuid: str
    title: str
    imdb_rating: float
    description: str
    genre: List
    actors: Optional[List]
    writers: Optional[List]
    directors: Optional[List]
    actors_names: Optional[List[str]]
    writers_names: Optional[List[str]]
