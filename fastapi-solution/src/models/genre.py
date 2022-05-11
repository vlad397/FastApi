from typing import ClassVar

from models.base import BaseMovie


class Genre(BaseMovie):
    """Модель жанров"""
    index: ClassVar[str] = 'genres'

    name: str
