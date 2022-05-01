import pytest
from ..conftest import expected_response_json
import inspect


@pytest.mark.asyncio
async def test_film_get_by_id(es_client, make_get_request, create_fill_delete_es_index):
    some_id = '3d825f60-9fff-4dfe-b294-1a45fa1e115d'
    method = '/films/'
    response = await make_get_request(f"{method}{some_id}", {})

    assert response.status == 200

    # Получить имя функции внутри функции для нахождения .json файла с таким же именем
    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)


@pytest.mark.asyncio
async def test_get_films(es_client, make_get_request, create_fill_delete_es_index):
    method = '/films/'
    response = await make_get_request(method, {})

    assert response.status == 200

    # Получить имя функции внутри функции для нахождения .json файла с таким же именем
    frame = inspect.currentframe()
    name = inspect.getframeinfo(frame).function
    assert response.body == expected_response_json(name)