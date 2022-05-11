import asyncio
import inspect
import json
from dataclasses import dataclass
from typing import Optional

import aiofiles
import aiohttp
import aioredis
import pytest
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from multidict import CIMultiDictProxy

from .settings import TestSettings

settings = TestSettings()


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int


@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=f'{settings.es_host}:{settings.es_port}')
    yield client
    await client.close()


@pytest.fixture(scope='session')
async def redis_client():
    redis = await aioredis.from_url(
        f'redis://{settings.redis_host}:{settings.redis_port}'
    )
    yield redis
    await redis.flushall()
    await redis.close()


@pytest.fixture(scope='session')
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture(scope='session')
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope='session')
async def create_fill_delete_es_index(es_client):
    for index in settings.es_indexes:
        # Создание индекса
        await create_index(es_client, index)
        # Заполнение данных для теста
        await fill_index(es_client, index)
    yield
    # Удаляет индекс
    for index in settings.es_indexes:
        await delete_index(es_client, index)


async def expected_response_json(name):
    if 'test_films' in name:
        file = f'{settings.expected_response_path}/films/{name}.json'
    elif 'test_genres' in name:
        file = f'{settings.expected_response_path}/genres/{name}.json'
    elif 'test_persons' in name:
        file = f'{settings.expected_response_path}/persons/{name}.json'
    async with aiofiles.open(file) as f:
        content = await f.read()
        response = json.loads(content)
    return response


async def create_index(es_client, index):
    """Создает индекс ES"""
    async with aiofiles.open(
        f'functional/testdata/schemes/{index}.json'
    ) as es_schema:
        settings, mappings = json.loads(await es_schema.read()).values()

    await es_client.indices.create(
        index=index, settings=settings, mappings=mappings
    )


async def fill_index(es_client, index):
    """Заполняет индекс тестовыми данными"""
    async with aiofiles.open(
        f'functional/testdata/load_data/{index}.json'
    ) as json_file:
        json_data = await json_file.read()
        data = json.loads(json_data)
    await async_bulk(es_client, data, index=index)


async def delete_index(es_client, index):
    """Удаляет индекс"""
    await es_client.indices.delete(index=index)


def get_function_name():
    """Возвращает название вызывающей функции"""
    name = inspect.stack()[1][3]
    return name


@pytest.fixture
def make_get_request(session):
    async def inner(
        method: str, params: Optional[dict] = None
    ) -> HTTPResponse:
        url = settings.service_url + settings.api_url + method
        async with session.get(url, params=params) as response:
            return HTTPResponse(
                body=await response.json(),
                headers=response.headers,
                status=response.status,
            )
    return inner
