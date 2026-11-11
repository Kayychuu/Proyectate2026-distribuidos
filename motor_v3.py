#!/usr/bin/env python3
"""ATEMPORALES V3: productor, workers Kafka, resultados y observabilidad."""
import argparse
import json
import math
import os
import random
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from scipy import stats
from scipy.optimize import minimize
from scipy.special import expit, logsumexp
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from funcion_generadora import generar_valor_hibrido

DEPTS = ("produccion", "rrhh", "inversion")
BOOTSTRAP = ["localhost:9092", "localhost:9094"]
BASE = Path(__file__).resolve().parent / ".atemporales_v3"
BASE.mkdir(exist_ok=True)


def ahora():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def leer_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def guardar_json(path, obj):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f".{p.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    os.replace(tmp, p)


def nuevo_evento(dept, ruido=2.5, prob=0.04, modo="mezcla_selectiva"):
    valor, comp, choque = generar_valor_hibrido(dept, modo_hibrido=modo, prob_choque=prob)
    return {"event_id": uuid.uuid4().hex, "timestamp_utc": ahora(), "area": dept,
            "valor": round(max(0.0, valor + random.gauss(0, ruido)), 3),
            "componente_latente": comp, "anomalia": bool(choque)}


def productor():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", retries=5,
                             linger_ms=12, value_serializer=lambda v: json.dumps(v).encode())
    print("[PRODUCTOR] Kafka conectado", flush=True)
    pendientes = deque()
    total = 0
    siguiente = time.monotonic()
    try:
        while True:
            cfg = leer_json(BASE / "control.json", {})
            tasa = max(1, min(800, int(cfg.get("tasa", 30))))
            prob = float(cfg.get("prob", 0.04))
            ruido = float(cfg.get("ruido", 2.5))
            modo = cfg.get("modo", "mezcla_selectiva")
            e = nuevo_evento(random.choice(DEPTS), ruido, prob, modo)
            pendientes.append(producer.send(f"empresa.{e['area']}", value=e, key=None))
            total += 1
            if len(pendientes) >= 50:
                for _ in range(25):
                    pendientes.popleft().get(timeout=15)  # verificar ACK, no éxito ficticio
            if total % 100 == 0:
                print(f"[PRODUCTOR] enviados={total} tasa_objetivo={tasa}/s", flush=True)
            siguiente += 1.0 / tasa
            retraso = siguiente - time.monotonic()
            if retraso > 0:
                time.sleep(retraso)
            else:
                siguiente = time.monotonic()
    finally:
        for f in pendientes:
            f.get(timeout=15)
        producer.flush()
        producer.close()


def ajustar(x):
    """AIC/BIC de densidades continuas; TCEV es mezcla de dos Gumbel."""
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 60 or np.ptp(x) <= 1e-9:
        return {"mejor": "sin datos suficientes", "modelos": {}, "n": len(x)}
    candidatos = []
    for nombre, dist, fijo in [
        ("Normal", stats.norm, False),
        ("Gumbel", stats.gumbel_r, False),
        ("LogNormal3P", stats.lognorm, False),
        ("BurrXII", stats.burr12, True),
    ]:
        try:
            pars = dist.fit(x, floc=0) if fijo else dist.fit(x)
            ll = float(np.sum(dist.logpdf(x, *pars)))
            k = len(pars) - int(fijo)
            if math.isfinite(ll):
                candidatos.append((nombre, 2*k-2*ll, k*np.log(len(x))-2*ll,
                                   [float(v) for v in pars], dist, pars))
        except (ValueError, RuntimeError, FloatingPointError):
            pass
    # 5 parámetros: peso, loc1, escala1, loc2, escala2.
    sd = max(float(np.std(x)), 0.1)
    q25, q75 = np.quantile(x, [0.25, 0.75])
    def negll(theta):
        w = expit(theta[0])
        l1, s1, l2, s2 = theta[1], np.exp(theta[2]), theta[3], np.exp(theta[4])
        a = np.log(w) + stats.gumbel_r.logpdf(x, loc=l1, scale=s1)
        b = np.log1p(-w) + stats.gumbel_r.logpdf(x, loc=l2, scale=s2)
        return float(-np.sum(np.logaddexp(a, b)))
    try:
        opt = minimize(negll, [0, q25, np.log(sd*.5), q75, np.log(sd*.5)],
                       method="L-BFGS-B", bounds=[(-5, 5), (float(x.min())-sd, float(x.max())+sd),
                       (np.log(.01), np.log(sd*10)), (float(x.min())-sd, float(x.max())+sd),
                       (np.log(.01), np.log(sd*10))], options={"maxiter": 90})
        if opt.success and math.isfinite(opt.fun):
            candidatos.append(("TCEV_Gumbel2", 10+2*opt.fun,
                               5*np.log(len(x))+2*opt.fun,
                               [float(v) for v in opt.x], None, None))
    except (ValueError, FloatingPointError):
        pass
    if not candidatos:
        return {"mejor": "sin ajuste", "modelos": {}, "n": len(x)}
    min_aic = min(c[1] for c in candidatos)
    pesos = np.exp(-.5*np.array([c[1]-min_aic for c in candidatos]))
    pesos = 100 * pesos / pesos.sum()
    modelos = {c[0]: {"aic": float(c[1]), "bic": float(c[2]),
                        "params": c[3], "peso_pct": float(peso)}
               for c, peso in zip(candidatos, pesos)}
    return {"mejor": min(modelos, key=lambda k: modelos[k]["aic"]),
            "modelos": modelos, "n": int(len(x))}


def worker(dept, wid):
    grupo = f"atemporales-v3-{dept}"
    consumer = KafkaConsumer(f"empresa.{dept}", bootstrap_servers=BOOTSTRAP,
                             group_id=grupo, auto_offset_reset="latest",
                             enable_auto_commit=True, max_poll_records=200,
                             value_deserializer=lambda b: json.loads(b.decode()))
    prod = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", retries=3,
                         value_serializer=lambda v: json.dumps(v).encode())
    vals = deque(maxlen=250)
    total = tp = fp = fn = tn = 0
    choques = 0
    latencias = deque(maxlen=200)
    eventos_recientes = deque(maxlen=50)
    detectores = {"mad": 0, "iforest": 0, "adwin": 0, "page_hinkley": 0}
    iforest = None
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError:
        IsolationForest = None
    try:
        from river.drift import ADWIN
        adwin = ADWIN(delta=.002)
    except ImportError:
        adwin = None
    ph_media = 0.0
    ph_sum = 0.0
    ph_min = 0.0
    last_fit = {"mejor": "esperando 60 observaciones", "modelos": {}, "n": 0}
    last_heartbeat = time.monotonic()
    start = time.monotonic()
    while True:
        paquetes = consumer.poll(timeout_ms=700, max_records=200)
        for mensajes in paquetes.values():
            for msg in mensajes:
                e = msg.value
                if not isinstance(e, dict) or e.get("area") != dept:
                    continue
                try:
                    v = float(e["valor"])
                    if not math.isfinite(v):
                        continue
                except (TypeError, KeyError, ValueError):
                    continue
                # Detección fuera de muestra: referencia de valores anteriores.
                xprev = np.asarray(vals, dtype=float)
                med = float(np.median(xprev)) if len(xprev) >= 30 else v
                mad = float(np.median(np.abs(xprev-med))) if len(xprev) >= 30 else 0
                anom = len(xprev) >= 30 and abs(v-med) > 3.5*max(1.4826*mad, 1.)
                iforest_flag = bool(iforest.predict([[v]])[0] == -1) if iforest is not None else False
                if iforest_flag:
                    detectores["iforest"] += 1
                if anom:
                    detectores["mad"] += 1
                if adwin is not None:
                    adwin.update(v)
                    if adwin.drift_detected:
                        detectores["adwin"] += 1
                # Page-Hinkley exploratorio sobre cambios de media
                ph_media += (v-ph_media)/(total+1)
                ph_sum += v-ph_media-0.1
                ph_min = min(ph_min, ph_sum)
                if ph_sum-ph_min > 50:
                    detectores["page_hinkley"] += 1
                    ph_sum = ph_min = 0
                real = bool(e.get("anomalia", False))
                tp += int(anom and real)
                fp += int(anom and not real)
                fn += int(not anom and real)
                tn += int(not anom and not real)
                choques += int(real)
                total += 1
                vals.append(v)
                eventos_recientes.append({"valor": v, "anomalia": real,
                                          "detectado": bool(anom), "area": dept,
                                          "offset": msg.offset, "particion": msg.partition})
                try:
                    dt = datetime.fromisoformat(e["timestamp_utc"])
                    latencias.append(max(0, (datetime.now(timezone.utc)-dt).total_seconds()*1000))
                except (KeyError, ValueError, TypeError):
                    pass
                # Aumentar tamaño para no ajustar mezclas en cada evento.
                if len(vals) >= 60 and total % 100 == 0:
                    last_fit = ajustar(vals)
                    if IsolationForest is not None:
                        iforest = IsolationForest(n_estimators=50, contamination=.05,
                                                   random_state=42).fit(np.array(vals).reshape(-1, 1))
                    result = {"fecha_utc": ahora(), "worker": f"{dept}-W{wid}",
                              "area": dept, "particion": msg.partition, "offset": msg.offset,
                              "total_worker": total, "reporte": last_fit,
                              "tp": tp, "fp": fp, "fn": fn, "tn": tn}
                    prod.send("empresa.resultados", value=result,
                              key=f"{dept}-{wid}".encode()).get(timeout=15)
        if time.monotonic() - last_heartbeat > 2:
            asignadas = sorted([p.partition for p in consumer.assignment() if p.topic == f"empresa.{dept}"])
            guardar_json(BASE / f"worker_{dept}_{wid}.json", {
                "actualizado": ahora(), "pid": os.getpid(), "worker": f"{dept}-W{wid}",
                "dept": dept, "asignadas": asignadas, "total": total,
                "uptime_s": round(time.monotonic()-start, 2), "choques": choques,
                "tp": tp, "fp": fp, "fn": fn, "tn": tn, "detectores": detectores,
                "latencia_p95_ms": round(float(np.percentile(latencias, 95)), 2) if latencias else None,
                "eventos": list(eventos_recientes), "ajuste": last_fit})
            last_heartbeat = time.monotonic()
            print(f"[{dept}-W{wid}] total={total} particiones={asignadas}", flush=True)


def colector():
    c = KafkaConsumer("empresa.resultados", bootstrap_servers=BOOTSTRAP,
                      group_id="atemporales-v3-colector", auto_offset_reset="earliest",
                      value_deserializer=lambda b: json.loads(b.decode()))
    salida = BASE / "resultados.jsonl"
    with salida.open("a", encoding="utf-8") as f:
        for m in c:
            f.write(json.dumps(m.value, ensure_ascii=False) + "\n")
            f.flush()


def metadatos():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=6000)
    try:
        cluster = admin.describe_cluster()
        nombres = [f"empresa.{x}" for x in DEPTS] + ["empresa.resultados"]
        raw_topics = admin.describe_topics(nombres)
        topics = []
        for t in raw_topics:
            topics.append({"topic": t.get("topic", t.get("name")), "partitions": [
                {"partition": p.get("partition", p.get("partition_index")),
                 "leader": p.get("leader", p.get("leader_id")),
                 "replicas": p.get("replicas", p.get("replica_nodes", [])),
                 "isr": p.get("isr", p.get("isr_nodes", []))}
                for p in t.get("partitions", [])]})
        retorno = {"brokers": cluster.get("brokers", []), "topics": topics, "grupos": {}}
        for dept in DEPTS:
            try:
                offsets = admin.list_group_offsets(f"atemporales-v3-{dept}").get(f"atemporales-v3-{dept}", {})
                retorno["grupos"][dept] = [
                    {"particion": p.partition, "offset_confirmado": o.offset}
                    for p, o in offsets.items() if p.topic == f"empresa.{dept}"
                ]
            except Exception:
                retorno["grupos"][dept] = []
        return retorno
    finally:
        admin.close()


def monitor():
    print(json.dumps(metadatos(), indent=2, ensure_ascii=False, default=str))



def benchmark(cantidad, tasa, timeout):
    """Encola una carga fija y mide hasta offsets confirmados por los grupos."""
    import csv
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    prod = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", retries=5,
                         linger_ms=5, value_serializer=lambda v: json.dumps(v).encode())
    inicio = time.monotonic()
    futuros = deque()
    siguiente = inicio
    for i in range(cantidad):
        dept = DEPTS[i % 3]
        e = nuevo_evento(dept)
        e["benchmark_id"] = f"{int(inicio)}-{i}"
        futuros.append(prod.send(f"empresa.{dept}", value=e, key=None))
        if len(futuros) >= 100:
            futuros.popleft().get(timeout=20)
        siguiente += 1/max(tasa, 1)
        time.sleep(max(0, siguiente-time.monotonic()))
    for f in futuros:
        f.get(timeout=20)
    prod.flush()
    prod.close()
    # Captura el último offset entregado por tópico y partición.
    consulta = KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                             group_id=None)
    objetivos = {}
    for dept in DEPTS:
        t = f"empresa.{dept}"
        ps = [TopicPartition(t, j) for j in consulta.partitions_for_topic(t)]
        objetivos[dept] = consulta.end_offsets(ps)
    consulta.close()
    completo = False
    while time.monotonic()-inicio < timeout:
        completo = True
        for dept in DEPTS:
            offsets = admin.list_group_offsets(f"atemporales-v3-{dept}").get(f"atemporales-v3-{dept}", {})
            for tp, final in objetivos[dept].items():
                comprometido = offsets.get(tp)
                if comprometido is None or comprometido.offset < final:
                    completo = False
                    break
        if completo:
            break
        time.sleep(1)
    duracion = time.monotonic()-inicio
    admin.close()
    activos = sum(1 for d in DEPTS for i in range(1, 4)
                  if (BASE/f"worker_{d}_{i}.pid").exists())
    fila = {"fecha_utc": ahora(), "workers": activos, "eventos": cantidad,
            "tasa_objetivo": tasa, "duracion_s": round(duracion, 3),
            "throughput_eventos_s": round(cantidad/duracion, 2),
            "completado": completo}
    destino = BASE/"benchmarks.csv"
    with destino.open("a", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=list(fila))
        if f.tell() == 0: wr.writeheader()
        wr.writerow(fila)
    print(json.dumps(fila, indent=2, ensure_ascii=False))
    if not completo:
        raise SystemExit("TIMEOUT: no todos los offsets fueron confirmados")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="accion", required=True)
    sub.add_parser("producer")
    w = sub.add_parser("worker")
    w.add_argument("--dept", required=True, choices=DEPTS)
    w.add_argument("--id", type=int, required=True)
    sub.add_parser("sink")
    sub.add_parser("monitor")
    b = sub.add_parser("bench")
    b.add_argument("--cantidad", type=int, default=1200)
    b.add_argument("--tasa", type=int, default=300)
    b.add_argument("--timeout", type=int, default=180)
    args = p.parse_args()
    if args.accion == "producer": productor()
    elif args.accion == "worker": worker(args.dept, args.id)
    elif args.accion == "sink": colector()
    elif args.accion == "monitor": monitor()
    elif args.accion == "bench": benchmark(args.cantidad, args.tasa, args.timeout)


if __name__ == "__main__":
    main()
