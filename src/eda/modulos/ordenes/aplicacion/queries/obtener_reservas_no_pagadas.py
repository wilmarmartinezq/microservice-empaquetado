from eda.seedwork.aplicacion.queries import Query, QueryHandler, ResultadoQuery
import uuid

class ObtenerOrdenesNoPagadas(Query):
    ...

class ObtenerOrdenesNoPagadasHandler(QueryHandler):

    def handle() -> ResultadoQuery:
        ...