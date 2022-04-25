from functools import lru_cache
from typing import List, Optional

from aioredis import Redis
from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from models.films import Film
from models.person import Person
from services.base import BaseService, SearchServiceMixin, build_redis_key
from services.films import FilmService

person_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class PersonService(BaseService, SearchServiceMixin):
    instance = Person

    async def get_film_list_by_id(self, base_id: str) -> Optional[List[Film]]:
        redis_key = build_redis_key(self.instance.index, instance_id=base_id)
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
        film_ids = instance.film_ids
        film_service = FilmService(self.redis, self.elastic)
        films = [await film_service.get_by_id(str(film_id)) for film_id in film_ids]

        return films

    async def query_builder(self, query_str: str = None):
        return {
            'match': {
                'full_name': {
                    'query': query_str,
                    'fuzziness': 'auto'
                }
            }
        }


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
