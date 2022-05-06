from http import HTTPStatus
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from helpers import static_texts
from models.response_models.films import Film_API, Film_Detail_API
from services.films import (FilmService, FilmsServices, get_film_service,
                            get_films_services)

router = APIRouter()


@router.get('/', summary='Получение списка фильмов',
            response_description='Краткая информация по каждому фильму')
@router.get('/search', summary='Поиск фильма по слову в названии',
            response_description='Краткая информация по фильму')
async def film_list(
        films_services: FilmsServices = Depends(get_films_services),
        query: Optional[str] = None,  # query param для поиска в названии
        genre: Optional[str] = None,  # query param для фильтрации по жанру
        sort: Optional[str] = '-imdb_rating',  # q_з для сортировки по рейтингу
        page_size: Optional[int] = 10,  # Количество объектов на странице
        page_number: Optional[int] = 1  # Номер страницы
) -> Optional[list]:
    """
    Выдает список объектов со следующей информацией:

    - **uuid**: UUID объекта в базе данных
    - **title**: Название кинопроизведения
    - **imdb_rating**: Рейтинг кинопроизведения

    Возможные запросы:

    - **query**: Параметр для поиска фильма по слову в названии
    - **genre**: Фильтрация фильмов определенного жанра
    - **sort**: Сортировка фильмов по рейтингу (-imdb_rating/imdb_rating)
    - **page_size**: Количество объектов на странице (По умолчанию - 10)
    - **page_number**: Номер страницы (По умолчанию - 1)

    При указании **query** невозможно дополнительно указать **genre**
    """
    if query:
        genre = None
        reverse = ''
    else:
        # Выбор варианта сортировки в зависимости от query param <sort>
        reverse = "desc" if sort[0] == '-' else "asc"
    films = await films_services.get_all_films(
        query, genre, reverse, page_size, page_number
    )

    return [Film_API(
        uuid=film['id'], title=film['title'],
        imdb_rating=film['imdb_rating']) for film in films
    ]


@router.get('/{film_id}', response_model=Film_Detail_API,
            summary='Получение фильма по uuid',
            response_description='Полная информация по фильму')
async def film_details(
        film_id: str,
        film_service: FilmService = Depends(get_film_service)
) -> Film_Detail_API:
    """
    Выдает объект со следующей информацией:

    - **uuid**: UUID объекта в базе данных
    - **title**: Название кинопроизведения
    - **imdb_rating**: Рейтинг кинопроизведения
    - **description**: Краткое описание кинопроизведения
    - **genre**: Список жанров
    - **actors**: Список актеров
    - **writers**: Список сценаристов
    - **director**: Режиссер
    """
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=static_texts.FILM_404
        )

    return Film_Detail_API(
        uuid=film.id, title=film.title,
        imdb_rating=film.imdb_rating, description=film.description,
        genre=film.genre, actors=film.actors, writers=film.writers,
        director=film.director
    )
