import inspect
import json

import pytest

from ..conftest import expected_response_json


@pytest.mark.asyncio
async def test_films_get_by_id(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение фильма по uuid"""
    method = '/films/'
    some_id = '3d825f60-9fff-4dfe-b294-1a45fa1e115d'
    response = await make_get_request(f"{method}{some_id}", {})

    assert response.status == 200

    # Получить имя функции внутри функции для нахождения
    # .json файла с таким же именем
    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_films_get_all(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Получение всех фильмов. Стандартом API выдает 10 фильмов на странице,
    отсортированных по убыванию рейтинга"""
    method = '/films/'
    response = await make_get_request(method, {})

    assert response.status == 200

    assert len(response.body) == 10

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_films_get_paginated_with_page_num(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на пагинацию по номеру страницы"""
    method = '/films'
    page_number = '?page_number=2'

    response = await make_get_request(f"{method}{page_number}", {})

    assert response.status == 200

    assert len(response.body) == 1

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_films_get_paginated_with_page_size(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на пагинацию по размеру страницы"""
    method = '/films'
    page_size = '?page_size=3'

    response = await make_get_request(f"{method}{page_size}", {})

    assert response.status == 200

    assert len(response.body) == 3

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_films_get_paginated_with_page_size_page_num(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на пагинацию по номеру страницы и размеру страницы"""
    method = '/films'
    page_number = '?page_number=1'
    page_size = '&page_size=5'

    response = await make_get_request(f"{method}{page_number}{page_size}", {})

    assert response.status == 200

    assert len(response.body) == 5

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_films_get_reverse_sorted(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на сортировку по возрастанию рейтинга"""
    method = '/films'
    sort = '?sort=imdb_rating'

    response = await make_get_request(f"{method}{sort}", {})

    assert response.status == 200

    assert len(response.body) == 10

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_films_get_by_genre(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на получение фильмов по жанру"""
    method = '/films'
    genre = '?genre=120a21cf-9097-479e-904a-13dd7198c1dd'

    response = await make_get_request(f"{method}{genre}", {})

    assert response.status == 200

    assert len(response.body) == 5

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_films_cache(
    es_client, redis_client, make_get_request, create_fill_delete_es_index
):
    """Тест кэша"""
    method = '/films'
    page_number = '?page_number=2'

    # Очищаем Redis, чтобы проверить, что после запроса один конкретный ключ
    await redis_client.flushall()
    assert await redis_client.dbsize() == 0

    response = await make_get_request(f"{method}{page_number}", {})

    assert response.status == 200

    redis_key = 'films||reverse::desc||page_number::2||page_size::10'
    assert await redis_client.exists(redis_key)
    redis_value = await redis_client.get(redis_key)
    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert json.loads(redis_value) == expected_response_json(name)


@pytest.mark.asyncio
async def test_films_search(
    es_client, make_get_request, create_fill_delete_es_index
):
    """Тест на поиск по ключевому слову"""
    method = '/films'
    search_query = '/search?query=lick'

    response = await make_get_request(f"{method}{search_query}", {})

    assert response.status == 200

    assert len(response.body) == 1

    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)
