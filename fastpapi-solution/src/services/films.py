import json
from functools import lru_cache
from typing import Optional

from aioredis import Redis
from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.films import Film
from services.base import BaseService, BaseListService
from pydantic import BaseModel


class FilmService(BaseService):
    """Выдача информации по фильму по uuid"""
    instance = Film

    async def get_by_id(self, film_id: str) -> Optional[Film]:
        """Основная функция поиска фильма по uuid"""
        # Пробуем взять фильм из кэша
        film = await self._instance_from_cache(film_id)
        if not film:
            # Если фильма нет, то ищем его в elasticsearch
            film = await self._get_instance_from_elastic(film_id)
            if not film:
                # Если фильма нигде нет, возвращаем None
                return None
            # Если фильм нашелся в elasticsearch, то сохраняем в кэше
            await self._put_instance_to_cache(film)

        return film


class FilmsServices(BaseListService):
    """Выдача информации по всем фильмам"""

    def get_elastic_query(
            self, query: Optional[str],
            genre: Optional[str], reverse: str
    ) -> dict:
        """Составление тела запроса для elasticsearch"""
        if genre:
            body = {"sort": [{"imdb_rating": {"order": reverse}}],
                    "query": {
                        "nested": {
                            "path": "genre",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"match": {"genre.uuid": genre}}
                                    ]
                                }
                            }
                        }
                    }
                    }
        elif query:
            body = {
                "query": {
                    "bool": {
                        "must":
                            {"match": {"title": {"query": query}}}
                    }
                },
            }
        else:
            body = {"sort": [{"imdb_rating": {"order": reverse}}],
                    "query": {"match_all": {}}}
        return body

    async def get_all_films(
            self, query: Optional[str], genre: Optional[str], reverse: str,
            page_size: int, page_number: int
    ) -> list:
        """Основная функция выдачи информации по фильмам"""
        # Создание составного ключа для хранения/поиска в кэше
        if query:
            redis_key = reverse + str(page_number) + str(page_size) + query
        elif genre:
            redis_key = reverse + str(page_number) + str(page_size) + genre
        else:
            redis_key = reverse + str(page_number) + str(page_size)
        redis_key = 'films' + redis_key
        # Ищем фильмы в кэше
        films = await self._instance_from_cache(redis_key)

        if not films:
            # Если в кэше нет, то ищем в elasticsearch
            films = await self._get_instance_from_elastic(
                query, genre, reverse, page_size, page_number)
            # Сохраняем в кэше информацию из elasticsearch
            await self._put_instance_to_cache(films, redis_key)
        return films

    async def _get_instance_from_elastic(
            self, query: Optional[str], genre: Optional[str], reverse: str,
            page_size: int, page_number: int
    ) -> list:
        """Поиск фильмов в elasticserch"""
        films_list = []
        # Попробуем найти фильмы, иначе вернем пустой список
        try:
            docs = await self.elastic.search(
                index='movies', size=page_size,
                from_=self.paginate_elastic(page_size, page_number),
                body=self.get_elastic_query(query, genre, reverse)
            )
            for doc in docs['hits']['hits']:
                # В каждом фильме оставим поля согласно модели
                # Объект модели нельзя сохранить в кэше,
                # потому делаем json() и json.loads()
                films_list.append(doc['_source'])
        except NotFoundError:
            return []
        return films_list


@lru_cache
def get_films_services(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmsServices:
    return FilmsServices(redis, elastic)


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
