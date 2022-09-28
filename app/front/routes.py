from app.front import views


# настраиваем пути, которые будут вести к нашей странице
def setup_routes(app):
    app.router.add_get("/", views.index)


def setup_image_out_routes(app):
    app.router.add_static('/image_out/',
                          path='scraper/image_out',
                          name='image_out')

