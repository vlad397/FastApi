import uuid
from http import HTTPStatus
from typing import List, Optional

from api.v1.films import Film_API
from fastapi import APIRouter, Depends, HTTPException
from helpers import static_texts
from models.response_models.person import Person
from services.persons import PersonService, get_person_service

router = APIRouter()


@router.get('/search', response_model=List[Person],
            summary='Поиск по персоне',
            response_description='Персоны, совпадающие с запросом')
async def person_search(
    person_service: PersonService = Depends(get_person_service),
    query: Optional[str] = None,
    page_number: Optional[int] = 1,
    page_size: Optional[int] = 50
) -> Optional[List[Person]]:
    """
    Выдает список объект со следующей информацией:

    - **uuid**: UUID объекта в базе данных
    - **full_name**: ФИО персоны
    - **role**: Роль персоны
    - **film_ids**: Список UUID кинопроизведений, в которых участвовал человек
    """
    hits = await person_service.search(query, page_number, page_size)
    if not hits:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=static_texts.PERSON_404
        )
    return [Person(
        uuid=hit.id, full_name=hit.full_name,
        role=hit.role, film_ids=hit.film_ids) for hit in hits]


@router.get('/{person_id}', response_model=Person,
            summary='Поиск персоны по uuid',
            response_description='Полная информация по персоне')
async def person_details(
    person_id: uuid.UUID,
    person_service: PersonService = Depends(get_person_service)
) -> Person:
    """
    Выдает объект со следующей информацией:

    - **uuid**: UUID объекта в базе данных
    - **full_name**: ФИО персоны
    - **role**: Роль персоны
    - **film_ids**: Список UUID кинопроизведений, в которых участвовал человек
    """
    person = await person_service.get_by_id(str(person_id))
    if not person:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=static_texts.PERSON_404
        )

    return Person(
        uuid=person.id, full_name=person.full_name,
        role=person.role, film_ids=person.film_ids
    )


@router.get('/{person_id}/film', response_model=List[Film_API],
            summary='Поиск фильмов по uuid персоны',
            response_description='Список фильмов с участием персоны')
async def person_film(
    person_id: uuid.UUID,
    person_service: PersonService = Depends(get_person_service)
) -> List[Film_API]:
    """
    Выдает список объектов со следующей информацией:

    - **uuid**: UUID объекта в базе данных
    - **title**: Название кинопроизведения
    - **imdb_rating**: Рейтинг кинопроизведения
    """
    films = await person_service.get_film_list_by_id(str(person_id))
    if not films:
        return []

    return [Film_API(
        uuid=film.id, title=film.title,
        imdb_rating=film.imdb_rating) for film in films]
