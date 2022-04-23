import uuid
from typing import ClassVar

from models.base import BaseMovie


class Person(BaseMovie):
    index: ClassVar[str] = 'persons'

    full_name: str
    role: str
    film_ids: list[uuid.UUID]
