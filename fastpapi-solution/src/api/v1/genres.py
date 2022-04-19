from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.genres import GenreService, get_genre_service, GenresServices, get_genres_services


router = APIRouter()


class Genre(BaseModel):
    uuid: str
    name: str


@router.get('/')
async def genre_list(genres_services: GenresServices = Depends(get_genres_services)) -> list:
    genres = await genres_services.get_all()

    return genres


@router.get('/{genre_id}', response_model=Genre)
async def genre_details(genre_id: str, genre_service: GenreService = Depends(get_genre_service)) -> Genre:
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')

    return Genre(uuid=genre.uuid, name=genre.name) 