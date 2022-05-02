import inspect
import json

import pytest

from ..conftest import expected_response_json


@pytest.mark.asyncio
async def test_genres_get_by_id(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение жанра по uuid"""
    method = '/genres/'
    some_id = '63c24835-34d3-4279-8d81-3c5f4ddb0cdc'
    response = await make_get_request(f"{method}{some_id}", {})

    assert response.status == 200

    # Получить имя функции внутри функции для нахождения
    # .json файла с таким же именем
    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_genres_get_all(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение всех жанров"""
    method = '/genres/'
    response = await make_get_request(method, {})

    assert response.status == 200
    assert len(response.body) == 5

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_genres_cache(
    es_client, redis_client, make_get_request, create_fill_delete_es_index
):
    """Тест кэша"""
    method = '/genres/'
    some_id = '63c24835-34d3-4279-8d81-3c5f4ddb0cdc'

    # Очищаем Redis, чтобы проверить, что после запроса один конкретный ключ
    await redis_client.flushall()
    assert await redis_client.dbsize() == 0

    response = await make_get_request(f"{method}{some_id}", {})

    assert response.status == 200

    assert await redis_client.exists(some_id)
    redis_value = await redis_client.get(some_id)
    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert json.loads(redis_value) == expected_response_json(name)
