from http import HTTPStatus
from typing import List, Optional

from api.v1.films import Film_API
from fastapi import APIRouter, Depends, HTTPException
from helpers import static_texts
from models.response_models.person import Person
from services.persons import PersonService, get_person_service

router = APIRouter()


@router.get('/search', response_model=List[Person])
async def person_search(
    person_service: PersonService = Depends(get_person_service),
    query: Optional[str] = None,
    page_number: Optional[int] = 1,
    page_size: Optional[int] = 50
) -> Optional[List[Person]]:
    hits = await person_service.search(query, page_number, page_size)
    if not hits:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=static_texts.PERSON_404
        )
    return [Person(
        uuid=hit.id, full_name=hit.full_name,
        role=hit.role, film_ids=hit.film_ids) for hit in hits]


@router.get('/{person_id}', response_model=Person)
async def person_details(
    person_id: str,
    person_service: PersonService = Depends(get_person_service)
) -> Person:
    person = await person_service.get_by_id(person_id)
    if not person:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=static_texts.FILM_404
        )

    return Person(
        uuid=person.id, full_name=person.full_name,
        role=person.role, film_ids=person.film_ids
    )


@router.get('/{person_id}/film', response_model=List[Film_API])
async def person_film(
    person_id: str,
    person_service: PersonService = Depends(get_person_service)
) -> List[Film_API]:
    films = await person_service.get_film_list_by_id(person_id)
    if not films:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=static_texts.FILM_404
        )

    return [Film_API(
        uuid=film.id, title=film.title,
        imdb_rating=film.imdb_rating) for film in films]
