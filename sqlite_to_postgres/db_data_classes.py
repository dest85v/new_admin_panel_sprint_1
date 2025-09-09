from uuid import UUID
from datetime import datetime, date
from dataclasses import dataclass


@dataclass
class FilmWork:
    id: UUID
    title: str
    description: str
    creation_date: date
    file_path: str
    rating: float
    type: str
    created: datetime
    modified: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)
        if isinstance(self.modified, str):
            self.modified = datetime.fromisoformat(self.modified)


@dataclass
class Person:
    id: UUID
    full_name: str
    created: datetime
    modified: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)
        if isinstance(self.modified, str):
            self.modified = datetime.fromisoformat(self.modified)


@dataclass
class Genre:
    id: UUID
    name: str
    description: str
    created: datetime
    modified: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)
        if isinstance(self.modified, str):
            self.modified = datetime.fromisoformat(self.modified)


@dataclass
class GenreFilmWork:
    id: UUID
    film_work_id: UUID
    genre_id: UUID
    created: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.genre_id, str):
            self.genre_id = UUID(self.genre_id)
        if isinstance(self.film_work_id, str):
            self.film_work_id = UUID(self.film_work_id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)


@dataclass
class PersonFilmWork:
    id: UUID
    film_work_id: UUID
    person_id: UUID
    role: str
    created: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.person_id, str):
            self.person_id = UUID(self.person_id)
        if isinstance(self.film_work_id, str):
            self.film_work_id = UUID(self.film_work_id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)
