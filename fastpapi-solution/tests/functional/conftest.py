import aiohttp
import asyncio
import pytest
import json
import aiofiles

from typing import Optional
from dataclasses import dataclass
from multidict import CIMultiDictProxy
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from . import settings




@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int

      
@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts='127.0.0.1:9200')
    yield client
    await client.close()
    

@pytest.fixture(scope='session')
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture(scope="session")
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
        


def expected_response_json(name):
    with open(f'functional/testdata/expected_response/{name}.json') as f:
        content = f.read()
        response = json.loads(content)
    return response



async def create_index(es_client, index):
    """Создает индекс ES"""
    with open(f'functional/testdata/schemes/{index}.json') as es_schema:
        settings, mappings = json.loads(es_schema.read()).values()
    
    await es_client.indices.create(index=index, settings=settings, mappings=mappings)


async def fill_index(es_client, index):
    """Заполняет индекс тествоыми данными"""
    with open(f'functional/testdata/load_data/{index}.json') as json_file:
        data = json.load(json_file)
    await async_bulk(es_client, data, index=index)


async def delete_index(es_client, index):
    """Удаляет индекс"""
    await es_client.indices.delete(index=index)


@pytest.fixture
def make_get_request(session):
    async def inner(method: str, params: Optional[dict] = None) -> HTTPResponse:
        url = settings.SERVICE_URL + settings.API_URL + method
        print(url)
        async with session.get(url, params=params) as response:
          return HTTPResponse(
            body=await response.json(),
            headers=response.headers,
            status=response.status,
          )
    return inner
