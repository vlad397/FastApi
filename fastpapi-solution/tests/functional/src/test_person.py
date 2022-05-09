import inspect
import json

import pytest

from .. import settings
from ..conftest import expected_response_json


@pytest.mark.asyncio
async def test_persons_get_by_id(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение персоны по uuid"""
    some_id = '01377f6d-9767-48ce-9e37-3c81f8a3c739'
    response = await make_get_request(
        f'{settings.METHOD_PERSONS}{some_id}', {}
    )

    assert response.status == 200

    # Получить имя функции внутри функции для нахождения
    # .json файла с таким же именем
    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_persons_get_films_by_id(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение фильмов uuid персоны"""
    some_id = 'aed21083-15dc-4e47-8d1e-63e52be9f6f0'
    response = await make_get_request(
        f'{settings.METHOD_PERSONS}{some_id}/film', {}
    )

    assert response.status == 200
    assert len(response.body) == 1

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_persons_search(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест поиска персоны"""
    query = 'search?query=john'
    response = await make_get_request(
        f'{settings.METHOD_PERSONS}{query}', {}
    )

    assert response.status == 200
    assert len(response.body) == 2

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_persons_cache(
    es_client, redis_client, make_get_request, create_fill_delete_es_index
):
    """Тест кэша"""
    some_id = '01377f6d-9767-48ce-9e37-3c81f8a3c739'

    # Очищаем Redis, чтобы проверить, что после запроса один конкретный ключ
    await redis_client.flushall()
    assert await redis_client.dbsize() == 0

    response = await make_get_request(
        f'{settings.METHOD_PERSONS}{some_id}', {}
    )

    assert response.status == 200

    assert await redis_client.exists(some_id)
    redis_value = await redis_client.get(some_id)
    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert json.loads(redis_value) == expected_response_json(name)
