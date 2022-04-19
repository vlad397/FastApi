from functools import lru_cache
from typing import Optional
import json

from aioredis import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.genre import Genre

GENRE_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class GenreService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, genre_id: str) -> Optional[Genre]:

        genre = await self._genre_from_cache(genre_id)
        if not genre:
            genre = await self._get_genre_from_elastic(genre_id)
            if not genre:
                return None
            await self._put_genre_to_cache(genre)

        return genre
    
    async def _get_genre_from_elastic(self, genre_id: str) -> Optional[Genre]:
        try:
            doc = await self.elastic.get('genres', genre_id)
            print(doc)
        except NotFoundError:
            return None
        return Genre(**doc['_source'])
    
    async def _genre_from_cache(self, genre_id: str) -> Optional[Genre]:
        data = await self.redis.get(genre_id)
        if not data:
            return None
        
        genre = Genre.parse_raw(data)
        return genre
    
    async def _put_genre_to_cache(self, genre: Genre):
        await self.redis.set(genre.uuid, genre.json(), expire=GENRE_CACHE_EXPIRE_IN_SECONDS)


class GenresServices:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
    
    async def get_all(self) -> list:
        genres = await self._genres_from_cache('genres')

        if not genres:
            genres = await self._get_genres_from_elastic()
            await self._put_genres_to_cache(genres)
        return genres
    
    async def _get_genres_from_elastic(self) -> list:
        genres_list = []
        try:
            docs = await self.elastic.search(index='genres', body={'size' : 26, "query":{"match_all":{}}})
            for doc in docs['hits']['hits']:
                genres_list.append(doc['_source'])
        except NotFoundError:
            return []
        return genres_list
    
    async def _genres_from_cache(self, key: str) -> Optional[list]:
        data = await self.redis.get(key)
        data = json.loads(data)
        if not data:
            return None
        
        return data
    
    async def _put_genres_to_cache(self, genres: list):
        await self.redis.set('genres', json.dumps(genres), expire=GENRE_CACHE_EXPIRE_IN_SECONDS)


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