from functools import lru_cache

from aioredis import Redis
from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.genre import Genre
from services.base import BaseListService, BaseService, build_redis_key


class GenreService(BaseService):
    """Выдача информации по жанру по uuid"""
    instance = Genre


class GenresServices(BaseListService):
    """Выдача информации по всем жанрам"""

    async def get_all(self, **kwargs) -> list:
        """Основная функция выдачи информации по всем жанрам"""
        # Ищем в кэше
        redis_key = build_redis_key('genres')
        instances = await self._instance_from_cache(redis_key)

        if not instances:
            # В кэше нет - ищем в es
            instances = await self._get_instance_from_elastic()
            # И сохраняем в кэше
            await self._put_instance_to_cache(instances, redis_key)
        return instances

    async def _get_instance_from_elastic(self) -> list:
        """Функция поиска жанров в es"""
        genres_list = []
        try:
            # Пробуем найти фильмы в es, иначе возвращаем пустой список
            docs = await self.elastic.search(
                index='genres', body={'size': 26, "query": {"match_all": {}}}
            )
            for doc in docs['hits']['hits']:
                genres_list.append(doc['_source'])
        except NotFoundError:
            return []
        return genres_list


@lru_cache()
def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)


@lru_cache()
def get_genres_services(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenresServices:
    return GenresServices(redis, elastic)
