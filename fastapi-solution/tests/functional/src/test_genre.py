import json
from http import HTTPStatus

import pytest

from ..conftest import expected_response_json, get_function_name
from ..settings import TestSettings

settings = TestSettings()

pytestmark = pytest.mark.asyncio


async def test_genres_get_by_id(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение жанра по uuid"""
    some_id = '63c24835-34d3-4279-8d81-3c5f4ddb0cdc'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(f'{settings.method_genres}{some_id}', {})

    assert response.status == HTTPStatus.OK
    assert response.body == result


async def test_genres_get_all(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение всех жанров"""
    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(settings.method_genres, {})

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 5
    assert response.body == result


async def test_genres_cache(
    es_client, redis_client, make_get_request, create_fill_delete_es_index
):
    """Тест кэша"""
    some_id = '63c24835-34d3-4279-8d81-3c5f4ddb0cdc'

    # Очищаем Redis, чтобы проверить, что после запроса один конкретный ключ
    await redis_client.flushall()
    assert await redis_client.dbsize() == 0

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(f'{settings.method_genres}{some_id}', {})
    status = await redis_client.exists(some_id)
    redis_value = await redis_client.get(some_id)

    assert response.status == HTTPStatus.OK
    assert status == 1
    assert json.loads(redis_value) == result


async def test_genres_uuid_validation(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на валидацию uuid"""
    some_id = '666'
    error_msg = {'detail': 'value is not a valid uuid'}

    response = await make_get_request(f'{settings.method_genres}{some_id}', {})

    assert response.status == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.body == error_msg


async def test_genres_non_existent_uuid(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на несуществующий жанр"""
    some_id = '2d811247-a406-32da-b34e-699bd10f7aec'
    error_msg = {'detail': 'Genre not found'}

    response = await make_get_request(f'{settings.method_genres}{some_id}', {})

    assert response.status == HTTPStatus.NOT_FOUND
    assert response.body == error_msg
