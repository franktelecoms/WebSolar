import os
import ssl
import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pandas as pd
import requests
from requests.adapters import HTTPAdapter

app = FastAPI(title="FusionSolar FullStack API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class HuaweiTLSAdapter(HTTPAdapter):

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


BASE_URL = "https://eu5.fusionsolar.huawei.com"
# Las credenciales se leerán de forma segura desde las variables de entorno de Render
USERNAME = os.getenv("HUAWEI_USER")
SYSTEM_CODE = os.getenv("HUAWEI_PASS")

# Caché en memoria (evita bloqueos por exceso de peticiones a Huawei)
CACHE = {"timestamp": 0, "payload": None}
TIEMPO_CACHE = 300  # 5 minutos


def df_a_lista_limpia(df: pd.DataFrame):
    if df.empty:
        return []
    return df.where(pd.notnull(df), None).to_dict(orient="records")


def ejecutar_extraccion_huawei():
    print(">>> Iniciando extracción de datos de FusionSolar...", flush=True)
    if not USERNAME or not SYSTEM_CODE:
        raise HTTPException(
            status_code=500,
            detail="Faltan las variables HUAWEI_USER o HUAWEI_PASS en Render.",
        )

    session = requests.Session()
    session.mount("https://", HuaweiTLSAdapter())
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    })

    # PASO 1: LOGIN Y REDIRECCIÓN
    nodo_actual = BASE_URL
    print(f">>> [1/5] Conectando y haciendo login en {nodo_actual}...", flush=True)
    login_res = session.post(
        f"{nodo_actual}/thirdData/login",
        json={"userName": USERNAME, "systemCode": SYSTEM_CODE},
        allow_redirects=False,
    )

    if login_res.status_code in (301, 302, 307, 308):
        nueva_url = login_res.headers.get("Location", "")
        print(f">>> Redirección detectada hacia: {nueva_url}", flush=True)
        if "fusionsolar.huawei.com" in nueva_url:
            nodo_actual = nueva_url.split("/thirdData")[0]
            time.sleep(2)
            login_res = session.post(
                f"{nodo_actual}/thirdData/login",
                json={"userName": USERNAME, "systemCode": SYSTEM_CODE},
                allow_redirects=False,
            )
        else:
            raise HTTPException(
                status_code=403,
                detail="Bloqueo WAF de Huawei o redirección inválida.",
            )

    token = login_res.headers.get("xsrf-token")
    if not token:
        print(f">>> Error en login: Status {login_res.status_code} | Respuesta: {login_res.text}", flush=True)
        raise HTTPException(
            status_code=401,
            detail=f"No se pudo iniciar sesión (Status {login_res.status_code})",
        )

    print(">>> Login exitoso. Token XSRF obtenido.", flush=True)
    session.headers.update({"xsrf-token": token})
    time.sleep(2)

    # PASO 2: PLANTAS + KPIS EN TIEMPO REAL
    print(">>> [2/5] Consultando listado de plantas...", flush=True)
    res_plantas = session.post(
        f"{nodo_actual}/thirdData/stations", json={"pageNo": 1, "pageSize": 100}
    ).json()
    plantas = res_plantas.get("data", {}).get("list", [])
    df_plantas = pd.DataFrame(plantas)
    print(f">>> Plantas encontradas: {len(df_plantas)}", flush=True)

    if df_plantas.empty:
        return {"plantas": [], "equipos": [], "telemetria": {}, "alarmas": []}

    all_codes = ",".join(df_plantas["plantCode"])
    time.sleep(2)

    print(">>> Consultando KPIs en tiempo real de las plantas...", flush=True)
    res_kpi = session.post(
        f"{nodo_actual}/thirdData/getStationRealKpi",
        json={"stationCodes": all_codes},
    ).json()
    filas_kpi = []
    for item in res_kpi.get("data") or []:
        datos = item.get("dataItemMap", {})
        datos["plantCode"] = item.get("stationCode")
        filas_kpi.append(datos)
    df_kpi = pd.DataFrame(filas_kpi)

    if not df_kpi.empty and "plantCode" in df_kpi.columns:
        df_resumen_plantas = pd.merge(
            df_plantas, df_kpi, on="plantCode", how="left"
        )
    else:
        df_resumen_plantas = df_plantas
    time.sleep(2)

    # PASO 3: INVENTARIO DE EQUIPOS
    print(">>> [3/5] Consultando inventario de equipos...", flush=True)
    res_equipos = session.post(
        f"{nodo_actual}/thirdData/getDevList", json={"stationCodes": all_codes}
    ).json()
    equipos = res_equipos.get("data") or []
    df_equipos = pd.DataFrame(equipos)
    print(f">>> Equipos encontrados: {len(df_equipos)}", flush=True)
    time.sleep(2)

    # PASO 4: TELEMETRÍA POR TIPO DE DISPOSITIVO
    print(">>> [4/5] Consultando telemetría por tipo de dispositivo...", flush=True)
    telemetria_por_tipo = {}
    if not df_equipos.empty and "devTypeId" in df_equipos.columns:
        for tipo_id in (1, 17, 39, 47):
            subset = df_equipos[df_equipos["devTypeId"] == tipo_id]
            if not subset.empty:
                ids_str = ",".join(subset["id"].astype(str))
                res_dev_kpi = session.post(
                    f"{nodo_actual}/thirdData/getDevRealKpi",
                    json={"devIds": ids_str, "devTypeId": tipo_id},
                ).json()
                filas_dev = []
                for d in res_dev_kpi.get("data") or []:
                    m = d.get("dataItemMap", {})
                    m["devId"] = d.get("devId")
                    filas_dev.append(m)
                if filas_dev:
                    telemetria_por_tipo[str(tipo_id)] = df_a_lista_limpia(
                        pd.DataFrame(filas_dev)
                    )
                time.sleep(2)

    # PASO 5: ALARMAS (ÚLTIMOS 7 DÍAS)
    print(">>> [5/5] Consultando alarmas de los últimos 7 días...", flush=True)
    ahora_ms = int(time.time() * 1000)
    hace_7_dias_ms = ahora_ms - (7 * 24 * 60 * 60 * 1000)
    res_alarmas = session.post(
        f"{nodo_actual}/thirdData/getAlarmList",
        json={
            "stationCodes": all_codes,
            "beginTime": hace_7_dias_ms,
            "endTime": ahora_ms,
            "language": "es_ES",
        },
    ).json()
    df_alarmas = pd.DataFrame(res_alarmas.get("data") or [])
    print(f">>> Extracción finalizada. Alarmas encontradas: {len(df_alarmas)}", flush=True)

    return {
        "ultima_actualizacion": time.strftime("%d/%m/%Y %H:%M:%S"),
        "plantas": df_a_lista_limpia(df_resumen_plantas),
        "equipos": df_a_lista_limpia(df_equipos),
        "telemetria": telemetria_por_tipo,
        "alarmas": df_a_lista_limpia(df_alarmas),
    }


@app.api_route("/", methods=["GET", "HEAD"])
def mostrar_web():
    return FileResponse("index.html")


@app.get("/api/datos")
def obtener_datos(forzar: bool = False):
    print(f">>> Petición recibida en /api/datos (forzar={forzar})", flush=True)
    ahora = time.time()
    if (
        not forzar
        and CACHE["payload"]
        and (ahora - CACHE["timestamp"] < TIEMPO_CACHE)
    ):
        print(">>> Devolviendo datos desde caché.", flush=True)
        return {**CACHE["payload"], "fuente": "caché"}

    resultado = ejecutar_extraccion_huawei()
    CACHE["payload"] = resultado
    CACHE["timestamp"] = ahora
    return {**resultado, "fuente": "en vivo"}
