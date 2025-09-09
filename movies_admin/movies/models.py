import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _

# Create your models here.


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        # Этот параметр указывает Django, что этот класс не является представлением таблицы
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    # Первым аргументом обычно идёт человекочитаемое название поля
    name = models.CharField(_('name'), max_length=255)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField(_('description'), blank=True)

    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = "content\".\"genre"
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField('full_name', max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('Persona')
        verbose_name_plural = _('Personas')

    def __str__(self):
        return self.full_name


class FilmWork(UUIDMixin, TimeStampedMixin):
    class TypeOfFilmWork(models.TextChoices):
        MOVIE = "movie", _("movie")
        TV_SHOW = "tv_show", _("tv_show")

    title = models.CharField(_('title'), max_length=255)
    description = models.CharField(_('description'), max_length=1024, blank=True)
    creation_date = models.DateField(_('creation_date'))
    rating = models.FloatField(_('rating'), blank=True,
                               validators=[
                                   MinValueValidator(0),
                                   MaxValueValidator(100)
                                   ])
    type = models.CharField(_('type'), max_length=50, choices=TypeOfFilmWork.choices, default=TypeOfFilmWork.MOVIE)
    #certificate = models.CharField('certificate', max_length=512, blank=True)
    # Параметр upload_to указывает, в какой подпапке будут храниться загружемые файлы. 
    # Базовая папка указана в файле настроек как MEDIA_ROOT
    #file_path = models.FileField('file', blank=True, null=True, upload_to='movies/')
    genres = models.ManyToManyField(Genre, through='GenreFilmWork')
    persons = models.ManyToManyField(Person, through='PersonFilmWork')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('Filmwork')
        verbose_name_plural = _('Filmworks')

    def __str__(self):
        return self.title


class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"


class PersonFilmWork(UUIDMixin):
    class Roles(models.TextChoices):
        PRODUCER = "producer", _("producer")
        DIRECTOR = "director", _("director")
        # SCREENWRITER = "screenwriter", _("screenwriter")
        CASTING_DIRECTOR = "casting director", _("casting director")
        PRODUCTION_DESIGNER = "production designer", _("production designer")
        COSTUME_DESIGNER = "costume designer", _("costume designer")
        MAKEUP_ARTIST = "makeup artist", _("makeup artist")
        COMPOSER = "composer", _("composer")
        ACTOR = "actor", _("actor")
        WRITER = "writer", _("writer")

    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.TextField(_('role'), max_length=255, choices=Roles.choices, default=Roles.PRODUCER)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
