import uuid
from http import HTTPStatus
from typing import List, Optional

from api.v1.films import Film_API
from fastapi import APIRouter, Depends, HTTPException
from models.base import BaseMovie
from services.persons import PersonService, get_person_service

router = APIRouter()


class Person(BaseMovie):
    full_name: str
    role: str
    film_ids: list


@router.get('/search', response_model=List[Person])
async def person_search(person_service: PersonService = Depends(get_person_service),
                        query: Optional[str] = None,
                        page_number: Optional[int] = 1,
                        page_size: Optional[int] = 50) -> Optional[List[Person]]:
    hits = await person_service.search_by_name(query, page_number, page_size)
    if not hits:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')
    return [Person(id=hit.id, full_name=hit.full_name, role=hit.role, film_ids=hit.film_ids) for hit in hits]


@router.get('/{person_id}', response_model=Person)
async def person_details(person_id: str, person_service: PersonService = Depends(get_person_service)) -> Person:
    person = await person_service.get_by_id(person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    return Person(id=person.id, full_name=person.full_name, role=person.role, film_ids=person.film_ids)


@router.get('/{person_id}/film', response_model=List[Film_API])
async def person_film(person_id: str, person_service: PersonService = Depends(get_person_service)) -> List[Film_API]:
    films = await person_service.get_film_list_by_id(person_id)
    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    return [Film_API(id=film.id, title=film.title, imdb_rating=film.imdb_rating) for film in films]
