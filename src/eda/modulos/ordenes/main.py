from fastapi import FastAPI
from pulsar.schema import *
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
import os
import datetime
import logging
import traceback
import pulsar, _pulsar
import aiopulsar
import asyncio
from pulsar.schema import *
from pydantic import BaseSettings


from typing import Any



epoch = datetime.datetime.utcfromtimestamp(0)

def time_millis():
    return int(time.time() * 1000)

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0

def millis_a_datetime(millis):
    return datetime.datetime.fromtimestamp(millis/1000.0)

def broker_host():
    return os.getenv('BROKER_HOST', default="localhost")



async def suscribirse_a_topico(topico: str, suscripcion: str, schema: Record, tipo_consumidor:_pulsar.ConsumerType=_pulsar.ConsumerType.Shared):
    try:
        async with aiopulsar.connect(f'pulsar://{broker_host()}:6650') as cliente:
            async with cliente.subscribe(
                topico, 
                consumer_type=tipo_consumidor,
                subscription_name=suscripcion, 
                schema=AvroSchema(schema)
            ) as consumidor:
                while True:
                    mensaje = await consumidor.receive()
                    print(mensaje)
                    datos = mensaje.value()
                    print(f'Evento recibido: {datos}')
                    await consumidor.acknowledge(mensaje)    

    except:
        logging.error(f'ERROR: Suscribiendose al tópico! {topico}, {suscripcion}, {schema}')
        traceback.print_exc()



class Despachador:
    def __init__(self):
        ...

    def publicar_mensaje(self, mensaje, topico):
        cliente = pulsar.Client(f'pulsar://{broker_host()}:6650')
        publicador = cliente.create_producer(topico, schema=AvroSchema(mensaje.__class__))
        publicador.send(mensaje)
        cliente.close()


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


client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer('evento-ordenes')
producer = client.create_producer('evento-aquisiciones')
producer = client.create_producer('evento-empaquetado')
producer = client.create_producer('comando-adquisiciones')
producer = client.create_producer('comando-empaquetado')

class Config(BaseSettings):
    APP_VERSION: str = "1"

settings = Config()


app = FastAPI()
tasks = list()

@app.on_event("startup")
async def app_startup():
    global tasks
    task1 = asyncio.ensure_future(suscribirse_a_topico("evento-ordenes", EventoOrden))
    task2 = asyncio.ensure_future(suscribirse_a_topico("evento-aquisiciones",  EventoAdquisiciones))
    task3 = asyncio.ensure_future(suscribirse_a_topico("evento-empaquetado", EventoEmpaquetado))        
    task4 = asyncio.ensure_future(suscribirse_a_topico("comando-ordenes", ComandoOrdenCrear))
    task5 = asyncio.ensure_future(suscribirse_a_topico("comando-adquisiciones", ComandoAdquisicionesCrear))
    task6 = asyncio.ensure_future(suscribirse_a_topico("comando-empaquetado", ComandoEmpaquetadoCrear))

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

