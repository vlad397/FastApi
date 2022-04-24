from functools import lru_cache
from typing import List, Optional

from aioredis import Redis
from api.v1.films import Film
from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.person import Person
from services.base import BaseService
from services.films import FilmService

person_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class PersonService(BaseService):
    instance = Person

    async def get_film_list_by_id(self, base_id: str) -> Optional[List[Film]]:
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
        film_ids = instance.film_ids
        film_service = FilmService(self.redis, self.elastic)
        films = [await film_service.get_by_id(str(film_id)) for film_id in film_ids]

        return films

    async def search_by_name(self,
                             query: Optional[str] = None,
                             page_number: Optional[int] = 1,
                             page_size: Optional[int] = 50) -> Optional[List[Person]]:

        try:
            doc = await self.elastic.search(
                index=self.instance.index,
                query={
                    'match': {
                        'full_name': {
                            'query': query,
                            'fuzziness': 'auto'
                        }
                    }
                },
                from_=self.paginate_elastic(page_size, page_number),
                size=page_size)
        except NotFoundError:
            return None
        if not doc:
            return None
        return [self.instance(**hit.get('_source')) for hit in doc.get('hits').get('hits')]


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)