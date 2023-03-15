from pulsar.schema import *
from .utils import time_millis
import uuid

class PagarOrdenPayload(Record):
    id_correlacion = String(),
    orden_id = String(),
    monto = Double()
    monto_vat = Double()
    fecha_creacion = Long()
 
class RevertirPagoPayload(Record):
    id = String()
    id_correlacion = String()
    orden_id = String()

class ComandoPagarOrden(Record):
    id = String(default=str(uuid.uuid4()))
    time = Long()
    ingestion = Long(default=time_millis())
    specversion = String(default="v1")
    type = String(default="ComandoPagarOrden")
    datacontenttype = String()
    service_name = String(default="pagos.eda")
    data = PagarOrdenPayload

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class ComandoRevertirPago(Record):
    id = String(default=str(uuid.uuid4()))
    time = Long()
    ingestion = Long(default=time_millis())
    specversion = String(default="v1")
    type = String(default="RevertirPagoOrden")
    datacontenttype = String()
    service_name = String(default="pagos.eda")
    data = RevertirPagoPayload

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
