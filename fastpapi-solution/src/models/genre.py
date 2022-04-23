from models.base import BaseMovie


class Genre(BaseMovie):
    """Модель жанров"""
    uuid: str   # todo rename
    name: str
