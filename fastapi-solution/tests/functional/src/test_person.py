import json
from http import HTTPStatus

import pytest

from ..conftest import expected_response_json, get_function_name
from ..settings import TestSettings

settings = TestSettings()

pytestmark = pytest.mark.asyncio


async def test_persons_get_by_id(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение персоны по uuid"""
    some_id = '01377f6d-9767-48ce-9e37-3c81f8a3c739'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_persons}{some_id}', {}
    )

    assert response.status == HTTPStatus.OK
    assert response.body == result


async def test_persons_get_films_by_id(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение фильмов uuid персоны"""
    some_id = 'aed21083-15dc-4e47-8d1e-63e52be9f6f0'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_persons}{some_id}/film', {}
    )

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 1
    assert response.body == result


async def test_persons_search(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест поиска персоны"""
    query = 'search?query=john'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_persons}{query}', {}
    )

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 2
    assert response.body == result


async def test_persons_cache(
    es_client, redis_client, make_get_request, create_fill_delete_es_index
):
    """Тест кэша"""
    some_id = '01377f6d-9767-48ce-9e37-3c81f8a3c739'

    # Очищаем Redis, чтобы проверить, что после запроса один конкретный ключ
    await redis_client.flushall()
    assert await redis_client.dbsize() == 0

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_persons}{some_id}', {}
    )
    status = await redis_client.exists(some_id)
    redis_value = await redis_client.get(some_id)

    assert response.status == HTTPStatus.OK
    assert status == 1
    assert json.loads(redis_value) == result


async def test_persons_uuid_validation(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на валидацию uuid"""
    some_id = '666'
    error_msg = {'detail': 'value is not a valid uuid'}

    response = await make_get_request(
        f'{settings.method_persons}{some_id}', {}
    )

    assert response.status == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.body == error_msg


async def test_persons_non_existent_uuid(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на несуществующую персону"""
    some_id = '2d811247-a406-32da-b34e-699bd10f7aec'
    error_msg = {'detail': 'Persons not found'}

    response = await make_get_request(
        f'{settings.method_persons}{some_id}', {}
    )

    assert response.status == HTTPStatus.NOT_FOUND
    assert response.body == error_msg


async def test_persons_films_by_non_existent_uuid(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на несуществующую персону"""
    some_id = '2d811247-a406-32da-b34e-699bd10f7aec'
    result = []

    response = await make_get_request(
        f'{settings.method_persons}{some_id}/film', {}
    )

    assert response.status == HTTPStatus.OK
    assert response.body == result


async def test_persons_search_by_non_existent_person(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на несуществующую персону"""
    query = 'search?query=Ivan'
    result = {'detail': 'Persons not found'}

    response = await make_get_request(
        f'{settings.method_persons}{query}', {}
    )

    assert response.status == HTTPStatus.NOT_FOUND
    assert response.body == result
