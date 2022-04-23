from functools import lru_cache
from typing import List, Optional, TypeVar

from aioredis import Redis
from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут

T = TypeVar("T", bound="BaseMovie")


class BaseService:
    """Базовый сервис. Включает подключение к редису и эластику, и основные методы.
    Использует дженерик для работы с моделью."""
    instance = T

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    def paginate_elastic(self, page_size: int, page_number: int) -> int:
        """Пагинация ответа elasticsearch"""
        if page_number == 1:
            return 0
        return page_size * (page_number - 1)

    # get_by_id возвращает объект фильма. Он опционален, так как фильм может отсутствовать в базе
    async def get_by_id(self, base_id: str) -> Optional[T]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        instance = await self._instance_from_cache(base_id)
        if not instance:
            # Если фильма нет в кеше, то ищем его в Elasticsearch
            instance = await self._get_instance_from_elastic(base_id)
            if not instance:
                # Если он отсутствует в Elasticsearch, значит, фильма вообще нет в базе
                return None
            # Сохраняем фильм  в кеш
            await self._put_instance_to_cache(instance)

        return instance

    async def get_list(self, sort: str) -> Optional[List[T]]:
        return []

    async def _get_instance_from_elastic(self, instance_id: str) -> Optional[T]:
        try:
            doc = await self.elastic.get(index=self.instance.index, id=instance_id)
        except NotFoundError:
            return None
        return self.instance(**doc['_source'])

    async def _instance_from_cache(self, instance_id: str) -> Optional[T]:
        # Пытаемся получить данные о фильме из кеша, используя команду get
        # https://redis.io/commands/get
        data = await self.redis.get(instance_id)
        if not data:
            return None

        # pydantic предоставляет удобное API для создания объекта моделей из json
        instance = self.instance.parse_raw(data)
        return instance

    async def _put_instance_to_cache(self, instance: T):
        # Сохраняем данные о фильме, используя команду set
        # Выставляем время жизни кеша — 5 минут
        # https://redis.io/commands/set
        # pydantic позволяет сериализовать модель в json
        await self.redis.set(instance.id, instance.json(), expire=FILM_CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_base_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic)
) -> BaseService:
    return BaseService(redis, elastic)
