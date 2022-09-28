import aiohttp_jinja2


# создаем функцию, которая будет отдавать html-файл
@aiohttp_jinja2.template("index.html")
async def index(request):
    return {'title': 'Цены на недвижимость в Крыму в 2022 году'}
