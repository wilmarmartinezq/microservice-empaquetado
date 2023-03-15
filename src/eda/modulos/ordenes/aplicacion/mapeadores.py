from eda.seedwork.aplicacion.dto import Mapeador as AppMap
from eda.seedwork.dominio.repositorios import Mapeador as RepMap
from eda.modulos.ordenes.dominio.entidades import Orden
from eda.modulos.ordenes.dominio.objetos_valor import Odo, Segmento, Leg
from .dto import OrdenDTO, OdoDTO, SegmentoDTO, LegDTO

from datetime import datetime


class MapeadorOrden(RepMap):
    _FORMATO_FECHA = '%Y-%m-%dT%H:%M:%SZ'


    def entidad_a_dto(self, entidad: Orden) -> OrdenDTO:
        
        fecha_creacion = entidad.fecha_creacion.strftime(self._FORMATO_FECHA)
        fecha_actualizacion = entidad.fecha_actualizacion.strftime(self._FORMATO_FECHA)
        _id = str(entidad.id)
        itinerarios = list()

    def dto_a_entidad(self, dto: OrdenDTO) -> Orden:
        orden = Orden()
   
        
        return orden



