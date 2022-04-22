from http import HTTPStatus
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.films import (FilmService, FilmsServices, get_film_service,
                            get_films_services)

router = APIRouter()


class Film_Detail(BaseModel):
    '''API модель для выдачи результата'''
    uuid: str
    title: str
    imdb_rating: float
    description: str
    genre: list
    actors: Optional[list]
    writers: Optional[list]
    directors: Optional[list]


@router.get('/')
@router.get('/search')
async def film_list(
    films_services: FilmsServices = Depends(get_films_services),
    query: Optional[str] = None,  # query param для поиска в названии
    genre: Optional[str] = None,  # query param для фильтрации по жанру
    sort: Optional[str] = '-imdb_rating',  # q_з для сортировки по рейтингу
    page_size: Optional[int] = 10,  # Количество объектов на странице
    page_number: Optional[int] = 1  # Номер страницы
) -> Optional[list]:
    if query:
        genre = None
        reverse = ''
    else:
        # Выбор варианта сортировки в зависимости от query param <sort>
        reverse = "desc" if sort[0] == '-' else "asc"
    films = await films_services.get_all_films(
        query, genre, reverse, page_size, page_number
    )

    return films


@router.get('/{film_id}', response_model=Film_Detail)
async def film_details(
    film_id: str,
    film_service: FilmService = Depends(get_film_service)
) -> Film_Detail:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='film not found'
        )

    return Film_Detail(
        uuid=film.uuid, title=film.title,
        imdb_rating=film.imdb_rating, description=film.description,
        genre=film.genre, actors=film.actors, writers=film.writers,
        directors=film.directors
    )
