from eda.seedwork.aplicacion.queries import QueryHandler
from eda.modulos.ordenes.infraestructura.fabricas import FabricaVista
from eda.modulos.ordenes.dominio.fabricas import FabricaOrdenes

class OrdenQueryBaseHandler(QueryHandler):
    def __init__(self):
        self._fabrica_vista: FabricaVista = FabricaVista()
        self._fabrica_ordenes: FabricaOrdenes = FabricaOrdenes()

    @property
    def fabrica_vista(self):
        return self._fabrica_vista
    
    @property
    def fabrica_ordenes(self):
        return self._fabrica_ordenes   