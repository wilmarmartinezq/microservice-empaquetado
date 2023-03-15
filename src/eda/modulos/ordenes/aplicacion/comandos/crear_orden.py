from eda.seedwork.aplicacion.comandos import Comando
from eda.modulos.ordenes.aplicacion.dto import OrdenDTO
from .base import CrearOrdenBaseHandler
from dataclasses import dataclass, field
from eda.seedwork.aplicacion.comandos import ejecutar_commando as comando

from eda.seedwork.infraestructura.uow import UnidadTrabajoPuerto

@dataclass
class CrearOrden(Comando):
    fecha_creacion: str
    fecha_actualizacion: str
    id: str


class CrearOrdenHandler(CrearOrdenBaseHandler):
    
    def handle(self, comando: CrearOrden):
        orden_dto = OrdenDTO(
                fecha_actualizacion=comando.fecha_actualizacion
            ,   fecha_creacion=comando.fecha_creacion
            ,   id=comando.id)

        orden: Orden = self.fabrica_ordenes.crear_objeto(orden_dto, MapeadorOrden())
        orden.crear_ordenes(orden)

        repositorio = self.fabrica_repositorio.crear_objeto(RepositorioOrdenes)
        repositorio_eventos = self.fabrica_repositorio.crear_objeto(RepositorioEventosOrdenes)

        UnidadTrabajoPuerto.registrar_batch(repositorio.agregar, orden, repositorio_eventos_func=repositorio_eventos.agregar)
        UnidadTrabajoPuerto.commit()


@comando.register(CrearOrden)
def ejecutar_comando_crear_orden(comando: CrearOrden):
    handler = CrearOrdenHandler()
    handler.handle(comando)
    