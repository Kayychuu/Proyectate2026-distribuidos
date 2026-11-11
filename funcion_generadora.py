import argparse
import json
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta

# Intento de carga opcional de kafka-python para publicar en el clúster multi-broker
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


# ============================================================
# DISTRIBUCIONES GENERADORAS BASE
# ============================================================

def generar_tcev():
    """
    TCEV (Two-Component Extreme Value): mezcla de dos distribuciones Gumbel
    para modelar tanto variaciones ordinarias como picos extraordinarios.
    """
    p = 0.15
    a1, u1 = 5.0, 40.0
    a2, u2 = 8.0, 60.0

    u = max(random.random(), 1e-12)
    if random.random() > p:
        return u1 - a1 * math.log(-math.log(u))
    return u2 - a2 * math.log(-math.log(u))


def generar_lognormal3p():
    """
    Log-Normal de tres parámetros con umbral de desplazamiento (gamma).
    """
    gamma = 10.0
    mu = 3.0
    sigma = 0.5
    z = random.gauss(0, 1)
    return gamma + math.exp(mu + sigma * z)


def generar_burr():
    """
    Burr XII de cuatro parámetros mediante transformada inversa (colas pesadas).
    """
    gamma = 5.0
    alpha = 30.0
    c = 2.0
    k = 2.0
    u = min(max(random.random(), 1e-12), 1.0 - 1e-12)
    return gamma + alpha * (((1.0 - u) ** (-1.0 / k) - 1.0) ** (1.0 / c))


# ============================================================
# MEZCLA PROBABILÍSTICA Y PONDERACIÓN DINÁMICA
# ============================================================

PESOS_BASE = {
    "produccion": {"tcev": 0.55, "lognormal3p": 0.25, "burr": 0.20},
    "rrhh": {"tcev": 0.20, "lognormal3p": 0.60, "burr": 0.20},
    "inversion": {"tcev": 0.20, "lognormal3p": 0.25, "burr": 0.55},
}


def generar_valor_hibrido(area, modo_hibrido="mezcla_selectiva", prob_choque=0.04):
    """
    Calcula el valor estocástico combinando TCEV, Log-Normal 3P y Burr XII,
    e inyecta choques de régimen (anomalías empresariales) con probabilidad controlada.
    """
    pesos = PESOS_BASE[area]
    r = random.random()

    if modo_hibrido == "combinacion_dirichlet":
        # Ponderación dinámica tipo Dirichlet centrada en los pesos del departamento
        alphas = [
            random.gammavariate(pesos["tcev"] * 10, 1.0),
            random.gammavariate(pesos["lognormal3p"] * 10, 1.0),
            random.gammavariate(pesos["burr"] * 10, 1.0),
        ]
        suma = sum(alphas) or 1.0
        w_tcev, w_ln, w_burr = [a / suma for a in alphas]
        valor = (
            w_tcev * generar_tcev()
            + w_ln * generar_lognormal3p()
            + w_burr * generar_burr()
        )
        componente = max(
            [("tcev", w_tcev), ("lognormal3p", w_ln), ("burr", w_burr)],
            key=lambda x: x[1],
        )[0]
    else:
        p_tcev = pesos["tcev"]
        p_ln = pesos["lognormal3p"]
        if r < p_tcev:
            valor = generar_tcev()
            componente = "tcev"
        elif r < p_tcev + p_ln:
            valor = generar_lognormal3p()
            componente = "lognormal3p"
        else:
            valor = generar_burr()
            componente = "burr"

    # Simulación de choque estocástico (cisne negro / pico operativo)
    es_anomalia = False
    if random.random() < prob_choque:
        es_anomalia = True
        valor *= random.uniform(1.45, 1.95)

    return valor, componente, es_anomalia


# ============================================================
# GENERADOR INFINITO DE EVENTOS EMPRESARIALES
# ============================================================

def generador_serie_hibrida(
    fecha_inicio=None,
    ruido_sigma=2.5,
    semilla=None,
    prob_choque=0.04,
):
    """
    Generador continuo (yield) de eventos empresariales sintéticos en formato JSON.
    """
    if semilla is not None:
        random.seed(semilla)

    areas = ["produccion", "rrhh", "inversion"]
    if fecha_inicio is None:
        tiempo_actual = datetime.now().replace(microsecond=0)
    else:
        tiempo_actual = datetime.fromisoformat(fecha_inicio)

    i = 1
    while True:
        t0 = time.perf_counter()
        area = random.choice(areas)

        # Lectura opcional de parámetros en vivo desde el panel de control Streamlit
        config_path = "/tmp/atemporales_state/sim_config.json"
        sigma_actual = ruido_sigma
        choque_actual = prob_choque
        modo_hibrido = "mezcla_selectiva"
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as cf:
                    cfg = json.load(cf)
                    sigma_actual = float(cfg.get("ruido_sigma", ruido_sigma))
                    choque_actual = float(cfg.get("prob_choque", prob_choque))
                    modo_hibrido = cfg.get("modo_hibrido", "mezcla_selectiva")
            except Exception:
                pass

        valor_base, componente_real, es_anomalia = generar_valor_hibrido(
            area, modo_hibrido=modo_hibrido, prob_choque=choque_actual
        )
        ruido = random.gauss(0, sigma_actual)
        valor_final = max(0.0, valor_base + ruido)
        latencia_ms = round((time.perf_counter() - t0) * 1000.0, 3)

        evento = {
            "event_id": f"EVT-{i:08d}",
            "timestamp": tiempo_actual.strftime("%Y-%m-%dT%H:%M:%S"),
            "emitted_at": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "area": area,
            "topic": f"empresa.{area}",
            "valor": round(valor_final, 2),
            "componente_latente": componente_real,
            "anomalia": es_anomalia,
            "latencia_ms": latencia_ms,
        }

        yield json.dumps(evento, ensure_ascii=False)

        tiempo_actual += timedelta(minutes=random.randint(15, 60))
        i += 1


# ============================================================
# EJECUCIÓN MULTI-BROKER (KAFKA CLUSTER + STREAM LOCAL)
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Productor Estocástico Híbrido para Clúster Apache Kafka KRaft"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092,localhost:9094",
        help="Lista de brokers de replicación Kafka",
    )
    parser.add_argument(
        "--state-dir",
        default="/tmp/atemporales_state",
        help="Directorio de telemetría compartida para workers/Streamlit",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.35,
        help="Segundos entre cada evento emitido",
    )
    parser.add_argument(
        "--ruido-sigma",
        type=float,
        default=2.5,
        help="Desviación estándar del ruido blanco gaussiano",
    )
    args = parser.parse_args()

    os.makedirs(args.state_dir, exist_ok=True)
    stream_file = os.path.join(args.state_dir, "eventos_stream.jsonl")

    producer = None
    if KAFKA_AVAILABLE:
        try:
            servers = [s.strip() for s in args.bootstrap_servers.split(",") if s.strip()]
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: v.encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=5,
            )
            sys.stderr.write(
                f"[Productor] Conectado al clúster Kafka en {servers} (acks=all)\n"
            )
        except Exception as exc:
            sys.stderr.write(
                f"[Productor] Aviso: Kafka cliente no disponible ({exc}). Usando bus local y stdout.\n"
            )

    try:
        for evento_json in generador_serie_hibrida(ruido_sigma=args.ruido_sigma):
            evento = json.loads(evento_json)
            topico = f"empresa.{evento['area']}"

            # 1. Publicar en el clúster multi-nodo de Kafka si está activo
            if producer is not None:
                try:
                    producer.send(topico, value=evento_json, key=evento["area"].encode("utf-8"))
                except Exception:
                    pass

            # 2. Registrar en el bus de eventos local para tolerancia total y telemetría
            try:
                with open(stream_file, "a", encoding="utf-8") as sf:
                    sf.write(evento_json + "\n")
            except Exception:
                pass

            # 3. Emitir por stdout (compatible con tuberías UNIX: python -u funcion_generadora.py | python -u worker_fit.py)
            print(evento_json, flush=True)

            # Revisar velocidad dinámica desde Streamlit
            intervalo = args.interval
            config_path = os.path.join(args.state_dir, "sim_config.json")
            if os.path.exists(config_path):
                try:
                    with open(config_path, "r", encoding="utf-8") as cf:
                        cfg = json.load(cf)
                        intervalo = float(cfg.get("interval", args.interval))
                except Exception:
                    pass

            time.sleep(max(0.05, intervalo))

    except KeyboardInterrupt:
        sys.stderr.write("\n[Productor] Generación detenida por el usuario.\n")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()