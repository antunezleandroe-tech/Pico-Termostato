from mqtt_as import MQTTClient, config
import uasyncio as asyncio
import machine
import dht
import ujson
import settings 
import ubinascii

# CONFIGURACIÓN DE HARDWARE:
PIN_DHT = 15
PIN_RELE = 16
PIN_LED = "LED"


pin_sensor = machine.Pin(PIN_DHT, machine.Pin.IN, machine.Pin.PULL_UP)
dht_sensor = dht.DHT11(pin_sensor)

rele = machine.Pin(PIN_RELE, machine.Pin.OUT, value=1)
led = machine.Pin(PIN_LED, machine.Pin.OUT)

ID_DISPOSITIVO = ubinascii.hexlify(machine.unique_id()).decode()
estado_actual = {"temperatura": 0.0, "humedad": 0.0}

# CONFIGURACIÓN DE MEMORIA NO VOLÁTIL (JSON):
db = {
    "setpoint": 24.0,
    "periodo": 10,
    "modo": "auto",
    "rele": "off"
}

def guardar_db():
    try:
        with open("parametros.json", "w") as f:
            ujson.dump(db, f)
    except OSError as e:
        print("Error guardando datos:", e)

def cargar_db():
    global db
    try:
        with open("parametros.json", "r") as f:
            db = ujson.load(f)
            print("Parámetros cargados:", db)
    except OSError:
        print("Creando archivo de parámetros...")
        guardar_db()

cargar_db()

# TAREAS ASÍNCRONAS:
async def tarea_destello():
    print("Orden de destello recibida")
    for _ in range(5):
        led.on()
        await asyncio.sleep(0.5)
        led.off()
        await asyncio.sleep(0.5)

async def tarea_sensor_publicador(client):
   
    await asyncio.sleep(5) 
    while True:
        try:

            dht_sensor.measure()
            estado_actual["temperatura"] = dht_sensor.temperature()
            estado_actual["humedad"] = dht_sensor.humidity()
            print(f"Lectura exitosa: T={estado_actual['temperatura']} H={estado_actual['humedad']}")
        except OSError:
          
            print("Error de lectura en DHT11. Reintentando en el próximo ciclo...")
            
        paquete = {
            "temperatura": estado_actual["temperatura"],
            "humedad": estado_actual["humedad"],
            "setpoint": db["setpoint"],
            "periodo": db["periodo"],
            "modo": db["modo"]
        }
        
        mensaje_json = ujson.dumps(paquete)
        print(f"Publicando: {mensaje_json}")
        await client.publish(ID_DISPOSITIVO, mensaje_json, qos=1)
        
        
        await asyncio.sleep(max(db["periodo"], 2))

async def tarea_control_rele():
    while True:
        modo = db["modo"]
        temp = estado_actual["temperatura"]
        if modo == "auto":
            sp = db["setpoint"]
            if temp > sp:
                rele.value(0) # ENCENDIDO
            elif temp < (sp - 0.5):
                rele.value(1) # APAGADO
        elif modo == "manual":
            rele.value(0 if db["rele"] == "on" else 1)
        await asyncio.sleep(1)

async def tarea_escucha_mqtt(client):
    await client.subscribe(f"{ID_DISPOSITIVO}/setpoint", 1)
    await client.subscribe(f"{ID_DISPOSITIVO}/periodo", 1)
    await client.subscribe(f"{ID_DISPOSITIVO}/destello", 1)
    await client.subscribe(f"{ID_DISPOSITIVO}/modo", 1)
    await client.subscribe(f"{ID_DISPOSITIVO}/rele", 1)
    
    async for topic, msg, retained in client.queue:
        topico_str = topic.decode()
        mensaje_str = msg.decode().strip()
        print(f"Recibido -> {topico_str}: {mensaje_str}")
        
        if topico_str.endswith("/setpoint"):
            db["setpoint"] = float(mensaje_str)
        elif topico_str.endswith("/periodo"):
            db["periodo"] = int(mensaje_str)
        elif topico_str.endswith("/modo"):
            db["modo"] = mensaje_str
        elif topico_str.endswith("/rele"):
            db["rele"] = mensaje_str
        elif topico_str.endswith("/destello"):
            asyncio.create_task(tarea_destello())
        
        guardar_db()

# FUNCIÓN PRINCIPAL:
async def main():
    config['ssid'] = settings.SSID
    config['wifi_pw'] = settings.password
    config['server'] = settings.BROKER
    config['port'] = 8883  
    config['ssl'] = True   
    config['queue_len'] = 10 
    
    MQTTClient.DEBUG = True
    client = MQTTClient(config)
    print(f"ID DISPOSTIVO:{ID_DISPOSITIVO}")
    print(f"Conectando a {settings.BROKER} via MQTTS...")
    await client.connect()
    
    print("Sistema en línea.")
    asyncio.create_task(tarea_sensor_publicador(client))
    asyncio.create_task(tarea_control_rele())
    asyncio.create_task(tarea_escucha_mqtt(client))
    
    while True:
        await asyncio.sleep(1)

try:
    asyncio.run(main())
finally:
    asyncio.new_event_loop()