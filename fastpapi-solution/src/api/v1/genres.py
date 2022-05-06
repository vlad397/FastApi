from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from helpers import static_texts
from models.response_models.genre import Genre
from services.genres import (GenreService, GenresServices, get_genre_service,
                             get_genres_services)

router = APIRouter()


@router.get('/', summary='Получение списка жанров',
            response_description='Список жанров')
async def genre_list(
    genres_services: GenresServices = Depends(get_genres_services)
) -> list:
    """
    Выдает список объектов со следующей информацией:

    - **uuid**: UUID объекта в базе данных
    - **name**: Название жанра
    """
    genres = await genres_services.get_all()

    return [Genre(uuid=genre['id'], name=genre['name']) for genre in genres]


@router.get('/{genre_id}', response_model=Genre,
            summary='Получение жанра по uuid',
            response_description='Полная информация по жанру')
async def genre_details(
    genre_id: str,
    genre_service: GenreService = Depends(get_genre_service)
) -> Genre:
    """
    Выдает объект со следующей информацией:

    - **uuid**: UUID объекта в базе данных
    - **name**: Название жанра
    """
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=static_texts.GENRE_404
        )

    return Genre(uuid=genre.id, name=genre.name)
