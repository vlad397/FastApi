from functools import lru_cache
from typing import Optional

from aioredis import Redis
from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.films import Film
from services.base import BaseListService, BaseService, build_redis_key


class FilmService(BaseService):
    """Выдача информации по фильму по uuid"""
    instance = Film


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
                                        {"match": {"genre.id": genre}}
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

    async def get_all(
            self, query: Optional[str], genre: Optional[str], reverse: str,
            page_size: int, page_number: int
    ) -> list:
        """Основная функция выдачи информации по фильмам"""
        # Создание составного ключа для хранения/поиска в кэше
        if query:
            redis_key = build_redis_key(
                'films', reverse=reverse, page_number=str(page_number),
                page_size=str(page_size), query=query
            )
        elif genre:
            redis_key = build_redis_key(
                'films', reverse=reverse, page_number=str(page_number),
                page_size=str(page_size), genre_id=genre
            )
        else:
            redis_key = build_redis_key(
                'films', reverse=reverse,
                page_number=str(page_number), page_size=str(page_size)
            )
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
