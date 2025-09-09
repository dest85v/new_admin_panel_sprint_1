from django.contrib import admin
from .models import Genre, FilmWork, GenreFilmWork, Person, PersonFilmWork
from django.utils.translation import gettext_lazy as _


# Register your models here.
@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    search_fields = ['name']


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    search_fields = ['full_name']


class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork
    autocomplete_fields = ['genre']


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork
    autocomplete_fields = ['person']


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline,)
    # Отображение полей в списке
    list_display = ('title', 'type', 'creation_date', 'rating', 'get_genres', 'get_persons')
    # Фильтрация в списке
    list_filter = ('type',)
    # Поиск по полям
    search_fields = ('title', 'description', 'id')
    #
    list_prefetch_related = ('genres', 'persons')

    def get_queryset(self, request):
        queryset = (
            super()
            .get_queryset(request)
            .prefetch_related(*self.list_prefetch_related)
        )
        return queryset

    def get_genres(self, obj):
        return ', '.join([genre.name for genre in obj.genres.all()])

    def get_persons(self, obj):
        persons_with_roles = []
        # Для локализацтт ролей
        roles_by_person = {}
        for person_film_work in obj.personfilmwork_set.all():
            roles_by_person[person_film_work.person_id] = person_film_work.get_role_display()
        for person in obj.persons.all():
            role = roles_by_person.get(person.id, _('undefined'))
            persons_with_roles.append(f"{person.full_name} ({role})")

        return ', '.join(persons_with_roles)

    get_genres.short_description = _('Film genres')
    get_persons.short_description = _('Persons with roles')
