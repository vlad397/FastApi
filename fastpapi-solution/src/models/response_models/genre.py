from .base import BaseResponse


class Genre(BaseResponse):
    """API модель для выдачи результата"""

    name: str
