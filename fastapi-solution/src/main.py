import logging

import aioredis
import uvicorn
from api.v1 import films, genres, persons
from core import config
from core.logger import LOGGING
from db import elastic, redis
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, ORJSONResponse

app = FastAPI(
    title='Read-only API для онлайн-кинотеатра',
    description=('Информация о фильмах, жанрах и людях, '
                 'участвовавших в создании произведения'),
    version="1.0.0",
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors()[0]['msg']}),
    )


@app.on_event('startup')
async def startup():
    redis.redis = aioredis.from_url(
        f'redis://{config.REDIS_HOST}:{config.REDIS_PORT}',
    )
    elastic.es = AsyncElasticsearch(
        hosts=[f'{config.ELASTIC_HOST}:{config.ELASTIC_PORT}']
    )
    if config.DEBUG:
        await redis.redis.flushdb()


@app.on_event('shutdown')
async def shutdown():
    await redis.redis.close()
    await elastic.es.close()


app.include_router(films.router, prefix='/api/v1/films', tags=['Фильмы'])
app.include_router(persons.router, prefix='/api/v1/persons', tags=['Люди'])
app.include_router(genres.router, prefix='/api/v1/genres', tags=['Жанры'])

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
        log_config=LOGGING,
        log_level=logging.DEBUG,
    )
