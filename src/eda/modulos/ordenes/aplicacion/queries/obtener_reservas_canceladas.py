from eda.seedwork.aplicacion.queries import Query, QueryHandler, ResultadoQuery
import uuid

class ObtenerOrdenesCanceladas(Query):
    ...

class ObtenerOrdenesCanceladasHandler(QueryHandler):

    def handle() -> ResultadoQuery:
        ...