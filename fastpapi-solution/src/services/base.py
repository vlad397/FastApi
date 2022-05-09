from abc import abstractmethod
import json
from functools import lru_cache
from typing import List, Optional, TypeVar

from aioredis import Redis
from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from fastapi.encoders import jsonable_encoder
from pydantic import parse_obj_as

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут

T = TypeVar("T", bound="BaseMovie")


def build_redis_key(*args, **kwargs):
    """Создание структурированного ключа Redis"""
    redis_key = ''
    if args:
        redis_key += '||'.join(args)
    for key, value in kwargs.items():
        redis_key += f'||{key}::{value}'
    return redis_key


class AbstractServiceClass:
    @abstractmethod
    async def _get_instance_from_elastic(self, **params):
        pass

    @abstractmethod
    async def _instance_from_cache(self, **params):
        pass

    @abstractmethod
    async def _put_instance_to_cache(self, **params):
        pass


class AbstractBaseServiceClass(AbstractServiceClass):
    @abstractmethod
    async def get_by_id(self, **params):
        pass


class AbstractBaseListServiceClass(AbstractServiceClass):
    @abstractmethod
    async def get_all(self, **params):
        pass


class SimpleService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    def paginate_elastic(self, page_size: int, page_number: int) -> int:
        """Пагинация ответа elasticsearch"""
        if page_number == 1:
            return 0
        return page_size * (page_number - 1)


class BaseService(SimpleService, AbstractBaseServiceClass):
    """Базовый сервис. Включает подключение к редису и эластику, и основные методы.
    Использует дженерик для работы с моделью."""
    instance = T

    # get_by_id возвращает объект фильма.
    # Он опционален, так как фильм может отсутствовать в базе
    async def get_by_id(self, base_id: str) -> Optional[T]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        instance = await self._instance_from_cache(base_id)
        if not instance:
            # Если фильма нет в кеше, то ищем его в Elasticsearch
            instance = await self._get_instance_from_elastic(base_id)
            if not instance:
                # Если он отсутствует в ES, значит, фильма вообще нет в базе
                return None
            # Сохраняем фильм  в кеш
            await self._put_instance_to_cache(instance)

        return instance

    async def _get_instance_from_elastic(
        self, instance_id: str
    ) -> Optional[T]:
        try:
            doc = await self.elastic.get(
                index=self.instance.index, id=instance_id
            )
        except NotFoundError:
            return None
        return self.instance(**doc['_source'])

    async def _instance_from_cache(self, instance_id: str) -> Optional[T]:
        # Пытаемся получить данные о фильме из кеша, используя команду get
        data = await self.redis.get(instance_id)
        if not data:
            return None

        instance = self.instance.parse_raw(data)
        return instance

    async def _put_instance_to_cache(self, instance: T):
        # Сохраняем данные о фильме, используя команду set
        # Выставляем время жизни кеша — 5 минут
        # https://redis.io/commands/set
        # pydantic позволяет сериализовать модель в json
        await self.redis.set(
            instance.id, instance.json(), expire=FILM_CACHE_EXPIRE_IN_SECONDS)


class SearchServiceMixin(SimpleService):
    instance = T

    async def query_builder(self, query_str: str = None):
        return {
            "query": {
                "query_string": {
                    "query": query_str
                }
            }
        }

    async def search(self,
                     query_str: str = None,
                     page_number: Optional[int] = 1,
                     page_size: Optional[int] = 50,
                     **kwargs) -> Optional[List[T]]:
        redis_key = build_redis_key(
            self.instance.index, query=query_str,
            page_number=str(page_number), page_size=str(page_size)
        )

        results = await self._get_search_from_cache(redis_key)
        if not results:
            results = await self._search_from_elastic(query_str)
            if not results:
                return None
            await self._put_search_to_cache(redis_key, results)
        return results

    async def _search_from_elastic(self,
                                   query_str: str = None,
                                   page_number: Optional[int] = 1,
                                   page_size: Optional[int] = 50,
                                   **kwargs) -> Optional[List[T]]:
        try:
            query = await self.query_builder(query_str)
            doc = await self.elastic.search(
                index=self.instance.index,
                query=query,
                from_=self.paginate_elastic(page_size, page_number),
                size=page_size)
        except NotFoundError:
            return None
        return [self.instance(
            **hit.get('_source')) for hit in doc.get('hits').get('hits')]

    async def _get_search_from_cache(self, redis_key: str) -> Optional[T]:
        data = await self.redis.get(redis_key)
        if not data:
            return None

        results = parse_obj_as(List[self.instance], json.loads(data))
        return results

    async def _put_search_to_cache(self, redis_key, results: List[T]):
        await self.redis.set(redis_key,
                             json.dumps(jsonable_encoder(results)),
                             expire=FILM_CACHE_EXPIRE_IN_SECONDS)


class BaseListService(SimpleService, AbstractBaseListServiceClass):
    """Базовый сервис. Включает подключение к редису и эластику,
    и основные методы.
    Использует дженерик для работы с моделью."""
    instance = T

    async def _instance_from_cache(self, key: str) -> Optional[list]:
        """Поиск фильмов в кэше"""
        data = await self.redis.get(key)
        if not data:
            return None
        data = json.loads(data)

        return data

    async def _put_instance_to_cache(self, instance: list, redis_key: str):
        """Сохранение фильмов в кэше"""
        await self.redis.set(
            redis_key, json.dumps(instance),
            expire=FILM_CACHE_EXPIRE_IN_SECONDS
        )


@lru_cache()
def get_base_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic)
) -> BaseService:
    return BaseService(redis, elastic)


@lru_cache()
def get_base_list_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic)
) -> BaseListService:
    return BaseListService(redis, elastic)
