from typing import Optional

from .base import BaseResponse


class Film_API(BaseResponse):
    """Упрощенный сериализатор фильма для списков"""

    title: str
    imdb_rating: float


class Film_Detail_API(BaseResponse):
    """API модель для выдачи результата"""

    title: str
    imdb_rating: float
    description: str
    genre: list
    actors: Optional[list]
    writers: Optional[list]
    director: Optional[list]
