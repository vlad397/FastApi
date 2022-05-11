from pydantic import BaseSettings, Field


class TestSettings(BaseSettings):
    es_host: str = Field('elasticsearch')
    es_port: int = Field(9200)
    redis_host: str = Field('redis')
    redis_port: int = Field(6379)
    service_url: str = Field('http://api:8000')
    es_indexes: list = Field(['movies', 'genres', 'persons'])
    api_url: str = Field('/api/v1')
    method_films: str = Field('/films/')
    method_films_query: str = Field('/films')
    method_genres: str = Field('/genres/')
    method_persons: str = Field('/persons/')
    expected_response_path: str = Field(
        'functional/testdata/expected_response')
