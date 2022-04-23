import json
from functools import lru_cache
from typing import Optional

from aioredis import Redis
from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.film import Film_Detail, Films

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class FilmService:
    """Выдача информации по фильму по uuid"""

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, film_id: str) -> Optional[Film_Detail]:
        """Основная функция поиска фильма по uuid"""
        # Пробуем взять фильм из кэша
        film = await self._film_from_cache(film_id)
        if not film:
            # Если фильма нет, то ищем его в elasticsearch
            film = await self._get_film_from_elastic(film_id)
            if not film:
                # Если фильма нигде нет, возвращаем None
                return None
            # Если фильм нашелся в elasticsearch, то сохраняем в кэше
            await self._put_film_to_cache(film)

        return film

    async def _get_film_from_elastic(
            self, film_id: str
    ) -> Optional[Film_Detail]:
        """Функция поиска фильма в elasticsearch"""
        try:
            doc = await self.elastic.get('movies', film_id)
        except NotFoundError:
            return None
        return Film_Detail(**doc['_source'])

    async def _film_from_cache(self, film_id: str) -> Optional[Film_Detail]:
        """Функция поиска фильма в кэше"""
        data = await self.redis.get(film_id)
        if not data:
            return None

        film = Film_Detail.parse_raw(data)
        return film

    async def _put_film_to_cache(self, film: Film_Detail):
        """Функция сохранения фильма в кэше для быстрого ответа"""
        await self.redis.set(
            film.uuid, film.json(), expire=FILM_CACHE_EXPIRE_IN_SECONDS
        )


class FilmsServices:
    """Выдача информации по всем фильмам"""

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    def paginate_elastic(self, page_size: int, page_number: int) -> int:
        """Пагинация ответа elasticsearch"""
        if page_number == 1:
            from_ = 0
        else:
            from_ = page_size * (page_number - 1)
        return from_

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
        # Ищем фильмы в кэше
        films = await self._films_from_cache('films' + redis_key)

        if not films:
            # Если в кэше нет, то ищем в elasticsearch
            films = await self._get_films_from_elastic(
                query, genre, reverse, page_size, page_number)
            # Сохраняем в кэше информацию из elasticsearch
            await self._put_films_to_cache(films, redis_key)
        return films

    async def _get_films_from_elastic(
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
                films_list.append(json.loads(Films(**doc['_source']).json()))

        except NotFoundError:
            return []
        return films_list

    async def _films_from_cache(self, key: str) -> Optional[list]:
        """Поиск фильмов в кэше"""
        data = await self.redis.get(key)
        if not data:
            return None
        data = json.loads(data)

        return data

    async def _put_films_to_cache(self, films: list, redis_key: str):
        """Сохранение фильмов в кэше"""
        await self.redis.set(
            'films' + redis_key, json.dumps(films),
            expire=FILM_CACHE_EXPIRE_IN_SECONDS
        )


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
