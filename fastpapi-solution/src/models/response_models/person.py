from .base import BaseResponse


class Person(BaseResponse):
    """API модель для выдачи результата"""

    full_name: str
    role: str
    film_ids: list
