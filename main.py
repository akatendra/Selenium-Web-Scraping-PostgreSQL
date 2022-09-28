from aiohttp import web  # основной модуль aiohttp
import jinja2  # шаблонизатор jinja2
import aiohttp_jinja2  # адаптация jinja2 к aiohttp


# в этой функции производится настройка url-путей для всего приложения
def setup_routes(application):
    from app.front.routes import setup_routes as setup_front_routes
    from app.front.routes import setup_image_out_routes
    setup_front_routes(application)  # настраиваем url-пути приложения front
    # setup_image_out_routes(application) # настраиваем url-пути к статическим файлам


def setup_external_libraries(application: web.Application) -> None:
    # указываем шаблонизатору, что html-шаблоны надо искать в папке templates
    aiohttp_jinja2.setup(application,
                         loader=jinja2.FileSystemLoader("templates"))


def setup_app(application):
    # настройка всего приложения состоит из:
    setup_external_libraries(
        application)  # настройки внешних библиотек, например шаблонизатора
    setup_routes(application)  # настройки роутера приложения


app = web.Application()  # создаем наш веб-сервер

if __name__ == "__main__":  # эта строчка указывает, что данный файл можно запустить как скрипт
    setup_app(app)  # настраиваем приложение
    web.run_app(app)  # запускаем приложение
