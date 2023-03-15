from pulsar.schema import *
from fastapi import FastAPI
import mysql.connector
import uuid
# from cliente.config.api import app_configs, settings
# from cliente.api.v1.router import router as v1

# from cliente.modulos.infraestructura.consumidores import suscribirse_a_topico
# from .eventos import EventoUsuario, UsuarioValidado, UsuarioDesactivado, UsuarioRegistrado, TipoCliente

# from cliente.modulos.infraestructura.despachadores import Despachador
# from cliente.seedwork.infraestructura import utils

import asyncio
import time
import traceback
import uvicorn

from typing import Any

from .consumidores import suscribirse_a_topico
from .despachadores import Despachador


def time_millis():
    return int(time.time() * 1000)


try:
    connection = mysql.connector.connect(host='localhost',
                                         database='ordenes',
                                         user='root',
                                         password='adminadmin')

    mySql_insert_query = """INSERT INTO ordenes (id, nombre, producto, cantidad, email, direccion, fecha_creacion) 
                           VALUES 
                           (1, 'Juan', 'Computador', 2, 'a@a.com', 'Calle', '2023-03-15') """

    cursor = connection.cursor()
    cursor.execute(mySql_insert_query)
    connection.commit()
    print(cursor.rowcount, "Record inserted successfully into ordenes table")
    cursor.close()

except mysql.connector.Error as error:
    print("Failed to insert record into ordenes table {}".format(error))

finally:
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")

class OrdenCreada(Record):
    id = String()
    nombre = String()
    producto = String()
    cantidad = int
    email = String()
    direccion = String()
    fecha_creacion = Long()

class OrdenCrear(Record):
    id = String()
    nombre = String()
    producto = String()
    cantidad = int
    email = String()
    direccion = String()
    fecha_creacion = Long()

class AdquisicionCreada(Record):
    id = String()
    producto = String()
    cantidad = int
    fecha_creacion = Long()

class AdquisicionCrear(Record):
    id = String()
    producto = String()
    cantidad = int
    fecha_creacion = Long()

class EmpaquetadoCreada(Record):
    id = String()
    producto = String()
    cantidad = int
    fecha_creacion = Long()

class EmpaquetadoCrear(Record):
    id = String()
    producto = String()
    cantidad = int
    fecha_creacion = Long()

class EventoOrden(Record):
    id = String(default=str(uuid.uuid4()))
    time = Long()
    ingestion = Long(default=time_millis())
    specversion = String(default="v1")
    type = String(default="EventoOrden")
    datacontenttype = String()
    service_name = String(default="orden.eda")
    orden_creada = OrdenCreada

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ComandoOrdenCrear(Record):
    id = String(default=str(uuid.uuid4()))
    time = Long()
    ingestion = Long(default=time_millis())
    specversion = String(default="v1")
    type = String(default="ComandoCrearOrden")
    datacontenttype = String()
    service_name = String(default="orden.eda")
    data = OrdenCrear

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)



class EventoAdquisiciones(Record):
    id = String(default=str(uuid.uuid4()))
    time = Long()
    ingestion = Long(default=time_millis())
    specversion = String(default="v1")
    type = String(default="EventoAdquisiciones")
    datacontenttype = String()
    service_name = String(default="orden.eda")
    adquisicion_creada = AdquisicionCreada

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ComandoAdquisicionesCrear(Record):
    id = String(default=str(uuid.uuid4()))
    time = Long()
    ingestion = Long(default=time_millis())
    specversion = String(default="v1")
    type = String(default="ComandoAdquisicionesCrear")
    datacontenttype = String()
    service_name = String(default="orden.eda")
    data = AdquisicionCrear

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class EventoEmpaquetado(Record):
    id = String(default=str(uuid.uuid4()))
    time = Long()
    ingestion = Long(default=time_millis())
    specversion = String(default="v1")
    type = String(default="EventoEmpaquetado")
    datacontenttype = String()
    service_name = String(default="empaquetado.eda")
    empaquetado_creada = EmpaquetadoCreada

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ComandoEmpaquetadoCrear(Record):
    id = String(default=str(uuid.uuid4()))
    time = Long()
    ingestion = Long(default=time_millis())
    specversion = String(default="v1")
    type = String(default="ComandoEmpaquetadoCrear")
    datacontenttype = String()
    service_name = String(default="empaquetado.eda")
    data =EmpaquetadoCrear

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class Config(BaseSettings):
    APP_VERSION: str = "1"

settings = Config()
app_configs: dict[str, Any] = {"title": "ordenes"}

app = FastAPI(**app_configs)
tasks = list()

@app.on_event("startup")
async def app_startup():
    global tasks
    task1 = asyncio.ensure_future(suscribirse_a_topico("evento-ordenes", "sub-ordenes", EventoOrden))
    task2 = asyncio.ensure_future(suscribirse_a_topico("evento-aquisiciones", "sub-adquisiciones", EventoAdquisiciones))
    task3 = asyncio.ensure_future(suscribirse_a_topico("evento-empaquetado", "sub-empaquetado", EventoEmpaquetado))        
    task4 = asyncio.ensure_future(suscribirse_a_topico("comando-ordenes", "sub-com-ordenes", ComandoOrdenCrear))
    task5 = asyncio.ensure_future(suscribirse_a_topico("comando-adquisiciones", "sub-com-adquisiciones", ComandoAdquisicionesCrear))
    task6 = asyncio.ensure_future(suscribirse_a_topico("comando-empaquetado", "sub-com-empaquetado", ComandoEmpaquetadoCrear))

    tasks.append(task1)
    tasks.append(task2)
    tasks.append(task3)
    tasks.append(task4)
    tasks.append(task5)
    tasks.append(task6)

@app.on_event("shutdown")
def shutdown_event():
    global tasks
    for task in tasks:
        task.cancel()

@app.get("/prueba-orden-creada", include_in_schema=False)
async def prueba_orden_crear() -> dict[str, str]:
    payload = OrdenCreada(
        id = "1",
        nombre = "Juan",
        producto = "Computador",
        cantidad = 1,
        email = "juan@a.com",
        direccion = "Calle sur",
        fecha_creacion = "15/03/2023"
    )

    evento = EventoOrden(
        time=time_millis(),
        ingestion=time_millis(),
        datacontenttype=OrdenCreada.__name__,
        orden_creada = payload
    )
    despachador = Despachador()
    despachador.publicar_mensaje(evento, "evento-ordenes")
    return {"status": "ok"}

    
@app.get("/prueba-orden-crear", include_in_schema=False)
async def prueba_orden_crear() -> dict[str, str]:
    payload = OrdenCrear(
        id = "1",
        nombre = "Juan",
        producto = "Computador",
        cantidad = 1,
        email = "juan@a.com",
        direccion = "Calle sur",
        fecha_creacion = "15/03/2023"
    )

    comando = ComandoOrdenCrear(
        time=time_millis(),
        ingestion=time_millis(),
        datacontenttype=OrdenCrear.__name__,
        data = payload
    )
    despachador = Despachador()
    despachador.publicar_mensaje(comando, "comando-ordenes")
    return {"status": "ok"}

@app.get("/prueba-adquisicion-creada", include_in_schema=False)
async def prueba_orden_crear() -> dict[str, str]:
    payload = AdquisicionCreada(
        id = "1",
        producto = "Computador",
        cantidad = 1,
        fecha_creacion = "15/03/2023"
    )

    evento = EventoAdquisiciones(
        time=time_millis(),
        ingestion=time_millis(),
        datacontenttype=AdquisicionCreada.__name__,
        orden_creada = payload
    )
    despachador = Despachador()
    despachador.publicar_mensaje(evento, "evento-aquisiciones")
    return {"status": "ok"}

    
@app.get("/prueba-adquisicion-crear", include_in_schema=False)
async def prueba_orden_crear() -> dict[str, str]:
    payload = AdquisicionCrear(
        id = "1",
        producto = "Computador",
        cantidad = 1,
        fecha_creacion = "15/03/2023"
    )

    comando = ComandoAdquisicionesCrear(
        time=time_millis(),
        ingestion=time_millis(),
        datacontenttype=AdquisicionCrear.__name__,
        data = payload
    )
    despachador = Despachador()
    despachador.publicar_mensaje(comando, "comando-adquisiciones")
    return {"status": "ok"}


@app.get("/prueba-empaquetado-creada", include_in_schema=False)
async def prueba_orden_crear() -> dict[str, str]:
    payload = EmpaquetadoCreada(
        id = "1",
        nombre = "Juan",
        producto = "Computador",
        cantidad = 1,
        fecha_creacion = "15/03/2023"
    )

    evento = EventoEmpaquetado(
        time=utils.time_millis(),
        ingestion=utils.time_millis(),
        datacontenttype=EmpaquetadoCreada.__name__,
        empaquetado_creada = payload
    )
    despachador = Despachador()
    despachador.publicar_mensaje(evento, "evento-empaquetado")
    return {"status": "ok"}

    
@app.get("/prueba-empaquetado-crear", include_in_schema=False)
async def prueba_empaquetado_crear() -> dict[str, str]:
    payload = OrdenCrear(
        id = "1",
        producto = "Computador",
        cantidad = 1,
        fecha_creacion = "15/03/2023"
    )

    comando = ComandoEmpaquetadoCrear(
        time=time_millis(),
        ingestion=time_millis(),
        datacontenttype=EmpaquetadoCrear.__name__,
        data = payload
    )
    despachador = Despachador()
    despachador.publicar_mensaje(comando, "comando-empaquetado")
    return {"status": "ok"}

