import json
from http import HTTPStatus

import pytest

from ..conftest import expected_response_json, get_function_name
from ..settings import TestSettings

settings = TestSettings()

pytestmark = pytest.mark.asyncio


async def test_films_get_by_id(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение фильма по uuid"""
    some_id = '3d825f60-9fff-4dfe-b294-1a45fa1e115d'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(f'{settings.method_films}{some_id}', {})

    assert response.status == HTTPStatus.OK
    assert response.body == result


async def test_films_get_all(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение всех фильмов. Стандартом API выдает 10 фильмов на странице,
    отсортированных по убыванию рейтинга"""
    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(settings.method_films, {})

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 10
    assert response.body == result


async def test_films_get_paginated_with_page_num(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на пагинацию по номеру страницы"""
    page_number = '?page_number=2'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_films_query}{page_number}', {}
    )

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 1
    assert response.body == result


async def test_films_get_paginated_with_page_size(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на пагинацию по размеру страницы"""
    page_size = '?page_size=3'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_films_query}{page_size}', {}
    )

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 3
    assert response.body == result


async def test_films_get_paginated_with_page_size_page_num(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на пагинацию по номеру страницы и размеру страницы"""
    page_number = '?page_number=1'
    page_size = '&page_size=5'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_films_query}{page_number}{page_size}', {}
    )

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 5
    assert response.body == result


async def test_films_get_reverse_sorted(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на сортировку по возрастанию рейтинга"""
    sort = '?sort=imdb_rating'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_films_query}{sort}', {}
    )

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 10
    assert response.body == result


async def test_films_get_by_genre(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на получение фильмов по жанру"""
    genre = '?genre=120a21cf-9097-479e-904a-13dd7198c1dd'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_films_query}{genre}', {}
    )

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 5
    assert response.body == result


async def test_films_cache(
    es_client, redis_client, make_get_request, create_fill_delete_es_index
):
    """Тест кэша"""
    page_number = '?page_number=2'
    redis_key = 'films||reverse::desc||page_number::2||page_size::10'

    # Очищаем Redis, чтобы проверить, что после запроса один конкретный ключ
    await redis_client.flushall()
    assert await redis_client.dbsize() == 0

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_films_query}{page_number}', {}
    )
    status = await redis_client.exists(redis_key)
    redis_value = await redis_client.get(redis_key)

    assert response.status == HTTPStatus.OK
    assert status == 1
    assert json.loads(redis_value) == result


async def test_films_search(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на поиск по ключевому слову"""
    search_query = '/search?query=lick'

    name = get_function_name()
    result = await expected_response_json(name)
    response = await make_get_request(
        f'{settings.method_films_query}{search_query}', {}
    )

    assert response.status == HTTPStatus.OK
    assert len(response.body) == 1
    assert response.body == result


async def test_films_uuid_validation(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на валидацию uuid"""
    some_id = '666'
    error_msg = {'detail': 'value is not a valid uuid'}

    response = await make_get_request(f'{settings.method_films}{some_id}', {})

    assert response.status == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.body == error_msg


async def test_films_non_existent_uuid(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на несуществующий фильм"""
    some_id = '2d811247-a406-32da-b34e-699bd10f7aec'
    error_msg = {'detail': 'Film not found'}

    response = await make_get_request(f'{settings.method_films}{some_id}', {})

    assert response.status == HTTPStatus.NOT_FOUND
    assert response.body == error_msg


async def test_films_non_existent_genre(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на выдачу пустого списка при несуществующем жанре"""
    genre = '?genre=130a21cf-9097-479e-904a-13dd7198c1gg'
    result = []

    response = await make_get_request(
        f'{settings.method_films_query}{genre}', {}
    )

    assert response.status == HTTPStatus.OK
    assert response.body == result


async def test_films_search_non_existent_title(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на выдачу пустого списка при несуществующем поисковом запросе"""
    search_query = '/search?query=Tomsk'
    result = []

    response = await make_get_request(
        f'{settings.method_films_query}{search_query}', {}
    )

    assert response.status == HTTPStatus.OK
    assert response.body == result
    