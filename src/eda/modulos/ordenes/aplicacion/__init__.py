from pydispatch import dispatcher

from .handlers import HandlerOrdenIntegracion

from eda.modulos.ordenes.dominio.eventos.ordenes import OrdenCreada, OrdenCancelada, OrdenAprobada, OrdenPagada

dispatcher.connect(HandlerOrdenIntegracion.handle_orden_creada, signal=f'{OrdenCreada.__name__}Integracion')
dispatcher.connect(HandlerOrdenIntegracion.handle_orden_cancelada, signal=f'{OrdenCancelada.__name__}Integracion')
dispatcher.connect(HandlerOrdenIntegracion.handle_orden_pagada, signal=f'{OrdenPagada.__name__}Integracion')
dispatcher.connect(HandlerOrdenIntegracion.handle_orden_aprobada, signal=f'{OrdenAprobada.__name__}Integracion')