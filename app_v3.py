#!/usr/bin/env python3
"""Observatorio Kafka + experimentación probabilística (solo datos observados)."""
import json
import socket
import time
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from scipy import stats
from scipy.special import expit
from kafka import KafkaConsumer, TopicPartition

from motor_v3 import BASE, DEPTS, BOOTSTRAP, leer_json, guardar_json, metadatos

st.set_page_config(page_title="ATEMPORALES • Live Cluster", page_icon="📡", layout="wide")
st.title("📡 ATEMPORALES — Observatorio de procesamiento paralelo")
st.caption("Kafka KRaft • 2 brokers locales • replicación RF=2 • 3 particiones por tópico • modelos probabilísticos")


def online(p):
    try:
        s = socket.create_connection(("127.0.0.1", p), timeout=.15)
        s.close()
        return True
    except OSError:
        return False


@st.cache_data(ttl=3, show_spinner=False)
def topologia():
    try:
        return metadatos(), ""
    except Exception as exc:
        return {}, str(exc)


def estados():
    resultado = []
    for dept in DEPTS:
        for i in range(1, 4):
            info = leer_json(BASE / f"worker_{dept}_{i}.json")
            if info:
                try:
                    t = pd.Timestamp(info["actualizado"])
                    info["activo"] = (pd.Timestamp.now(tz="UTC")-t).total_seconds() < 8
                except Exception:
                    info["activo"] = False
                resultado.append(info)
    return resultado


def tabla_particiones(meta):
    filas = []
    for topic in meta.get("topics", []):
        for p in topic.get("partitions", []):
            replicas = p.get("replicas", [])
            isr = p.get("isr", [])
            filas.append({"Tópico": topic["topic"], "P": p["partition"],
                          "Líder": p.get("leader"), "Réplicas": str(replicas),
                          "ISR": str(isr), "Sincronizadas": len(isr),
                          "Bajo réplica": len(isr) < len(replicas)})
    return pd.DataFrame(filas)


def retraso(meta):
    out = []
    c = None
    try:
        c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=None,
                          enable_auto_commit=False, request_timeout_ms=6000)
        for dept in DEPTS:
            topic = f"empresa.{dept}"
            for x in meta.get("topics", []):
                if x["topic"] != topic:
                    continue
                parts = [TopicPartition(topic, p["partition"]) for p in x["partitions"]]
                finales = c.end_offsets(parts)
                confirmados = {r["particion"]: r["offset_confirmado"]
                               for r in meta.get("grupos", {}).get(dept, [])}
                for tp, fin in finales.items():
                    off = confirmados.get(tp.partition)
                    out.append({"Departamento": dept, "Partición": tp.partition,
                                "Pendientes": max(0, fin-off) if off is not None and off >= 0 else None})
    except Exception:
        return pd.DataFrame(out)
    finally:
        if c: c.close()
    return pd.DataFrame(out)


def diagrama(meta, ws):
    parts = tabla_particiones(meta)
    if parts.empty:
        return None
    nodos = ["Generador", "Broker 2 (:9092)", "Broker 3 (:9094)"]
    enlaces = []
    for _, p in parts.iterrows():
        if p["Tópico"] == "empresa.resultados":
            continue
        nodo = f"{p['Tópico'].split('.')[-1]}:P{p['P']}"
        nodos.append(nodo)
        lider = {2: "Broker 2 (:9092)", 3: "Broker 3 (:9094)"}.get(p["Líder"])
        if lider:
            enlaces.extend([("Generador", lider, 1), (lider, nodo, 1)])
        for w in ws:
            if w.get("activo") and w["dept"] == p["Tópico"].split(".")[-1] and p["P"] in w.get("asignadas", []):
                nodos.append(w["worker"])
                enlaces.append((nodo, w["worker"], 1))
    nodos.append("empresa.resultados")
    for w in ws:
        if w.get("activo"):
            enlaces.append((w["worker"], "empresa.resultados", 1))
    nodos = list(dict.fromkeys(nodos))
    if not enlaces:
        return None
    idx = {v: i for i, v in enumerate(nodos)}
    fig = go.Figure(go.Sankey(
        arrangement="snap", node=dict(label=nodos, pad=19, thickness=14),
        link=dict(source=[idx[a] for a,b,v in enlaces],
                  target=[idx[b] for a,b,v in enlaces],
                  value=[v for a,b,v in enlaces])))
    fig.update_layout(height=490, margin=dict(l=10,r=10,t=20,b=15))
    return fig


with st.sidebar:
    st.subheader("🎛️ Generación en vivo")
    cfg = leer_json(BASE / "control.json", {"tasa":30, "prob":.04, "ruido":2.5})
    tasa = st.slider("Eventos por segundo (objetivo)", 1, 800, int(cfg.get("tasa",30)))
    prob = st.slider("Probabilidad de choque", 0., .50, float(cfg.get("prob",.04)), .01)
    ruido = st.slider("Ruido σ", 0., 15., float(cfg.get("ruido",2.5)), .5)
    modo = st.selectbox("Mezcla", ["mezcla_selectiva", "combinacion_dirichlet"])
    if st.button("Aplicar al productor", use_container_width=True):
        guardar_json(BASE / "control.json", {"tasa":tasa, "prob":prob, "ruido":ruido, "modo":modo})
        st.success("Configuración enviada al productor")
    st.divider()
    st.info("Para escalar workers o simular fallos usa los comandos de terminal. Este panel no detiene brokers.")

vista = st.tabs(["🌐 Clúster", "⚡ Paralelización", "🧠 Probabilidad", "🚨 Anomalías", "📚 Experimentos"])


@st.fragment(run_every="3s")
def panel_vivo():
    meta, error = topologia()
    ws = estados()
    part = tabla_particiones(meta)
    df_lag = retraso(meta) if meta else pd.DataFrame()
    activos = [w for w in ws if w.get("activo")]
    lag = df_lag["Pendientes"].sum() if not df_lag.empty else None
    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Controller :9093", "🟢" if online(9093) else "🔴")
    k2.metric("Broker 2 :9092", "🟢" if online(9092) else "🔴")
    k3.metric("Broker 3 :9094", "🟢" if online(9094) else "🔴")
    k4.metric("Workers activos", len(activos))
    if error:
        st.error(f"Kafka no responde: {error}")
        return
    st.metric("Consumer lag total (offsets pendientes)", "No disponible" if lag is None else f"{lag:,.0f}")
    fig = diagrama(meta, ws)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    st.caption("El Sankey muestra rutas de liderazgo y asignación, no bytes ni eventos reales por enlace. La replicación se detalla abajo.")
    st.dataframe(part, use_container_width=True, hide_index=True)
    st.subheader("Lag por partición")
    if not df_lag.empty:
        st.bar_chart(df_lag.pivot_table(index="Departamento", columns="Partición", values="Pendientes", aggfunc="sum"))
    else:
        st.info("Todavía no existen offsets confirmados de los grupos de consumidores.")


@st.fragment(run_every="3s")
def panel_paralelo():
    ws = estados()
    if not ws:
        st.info("Esperando métricas de los workers Kafka...")
        return
    ahora_s = time.time()
    totals = sum(w.get("total",0) for w in ws if w.get("activo"))
    hist = st.session_state.setdefault("historial_tasas", [])
    if hist and ahora_s > hist[-1][0]:
        dt = ahora_s-hist[-1][0]
        tasa = max(0, (totals-hist[-1][1])/dt)
    else: tasa = 0
    hist.append((ahora_s, totals, tasa))
    del hist[:-70]
    a,b,c = st.columns(3)
    a.metric("Eventos procesados (esta sesión)", f"{totals:,}")
    b.metric("Rendimiento observado", f"{tasa:.1f} ev/s")
    c.metric("Workers activos", sum(w.get("activo",False) for w in ws))
    if len(hist)>2:
        df = pd.DataFrame(hist, columns=["t","total","eventos_segundo"])
        st.line_chart(df.set_index("t")["eventos_segundo"])
    st.dataframe(pd.DataFrame([{"Worker":w["worker"],"Activo":w["activo"],
       "Particiones":w.get("asignadas", []),"Procesados":w.get("total",0),
       "Latencia p95 (ms)":w.get("latencia_p95_ms")} for w in ws]),
       hide_index=True,use_container_width=True)
    st.caption("Tasa calculada a partir de diferencias de contadores por sesión; tras reinicios puede reinicializarse.")


@st.fragment(run_every="4s")
def panel_modelos():
    ws = [w for w in estados() if w.get("activo")]
    dept = st.selectbox("Departamento", DEPTS, key="dept_prob")
    sub = [w for w in ws if w["dept"] == dept]
    modelos = []
    for w in sub:
        a = w.get("ajuste", {})
        for nombre, info in a.get("modelos", {}).items():
            modelos.append({"Worker": w["worker"], "Modelo":nombre,
                "AIC":info["aic"],"BIC":info["bic"],"Peso Akaike (%)":info["peso_pct"]})
    if modelos:
        st.dataframe(pd.DataFrame(modelos).sort_values(["Worker","AIC"]),
                     use_container_width=True, hide_index=True)
    else: st.info("Esperando al menos 60 observaciones por worker y un ajuste.")
    valores = [e["valor"] for w in sub for e in w.get("eventos", [])]
    if valores:
        fig = go.Figure(go.Histogram(x=valores, nbinsx=20, histnorm="probability density"))
        fig.update_layout(title=f"Muestra reciente • {dept}", xaxis_title="Valor", yaxis_title="Densidad")
        st.plotly_chart(fig, use_container_width=True)
    st.caption("TCEV es una mezcla estimada de dos Gumbel. AIC/BIC comparan densidades; el ajuste es exploratorio.")


@st.fragment(run_every="4s")
def panel_anomalias():
    ws = [w for w in estados() if w.get("activo")]
    if not ws:
        st.info("Esperando detecciones...")
        return
    tp = sum(w.get("tp",0) for w in ws)
    fp = sum(w.get("fp",0) for w in ws)
    fn = sum(w.get("fn",0) for w in ws)
    tn = sum(w.get("tn",0) for w in ws)
    precision = tp/(tp+fp) if (tp+fp) else 0
    recall = tp/(tp+fn) if (tp+fn) else 0
    f1 = 2*precision*recall/(precision+recall) if (precision+recall) else 0
    a,b,c,d = st.columns(4)
    a.metric("Choques inyectados", tp+fn)
    b.metric("Precisión MAD", f"{precision:.3f}")
    c.metric("Recall MAD", f"{recall:.3f}")
    d.metric("F1 MAD", f"{f1:.3f}")
    st.dataframe(pd.DataFrame([{"TP":tp,"FP":fp,"FN":fn,"TN":tn}]), hide_index=True)
    st.dataframe(pd.DataFrame([{"Worker":w["worker"],**w.get("detectores",{})} for w in ws]),
                 hide_index=True, use_container_width=True)
    st.caption("MAD es el detector evaluado con etiquetas del generador. Isolation Forest, Page-Hinkley y ADWIN son exploratorios; ADWIN requiere river instalado.")


def panel_experimentos():
    csv = BASE / "benchmarks.csv"
    if csv.exists():
        df = pd.read_csv(csv)
        st.dataframe(df, use_container_width=True, hide_index=True)
        base = df[df["workers"]==3]
        if not base.empty:
            b = float(base.iloc[-1]["throughput_eventos_s"])
            df["speedup_aprox"] = df["throughput_eventos_s"]/b if b else np.nan
            df["eficiencia_aprox"] = df["speedup_aprox"]/(df["workers"]/3)
            st.line_chart(df.set_index("workers")[["speedup_aprox", "eficiencia_aprox"]])
        st.download_button("Descargar benchmarks CSV", csv.read_bytes(), "benchmarks.csv")
    else:
        st.info("Realiza los ensayos con el comando bash experimento_v3.sh")
    archivo = BASE / "resultados.jsonl"
    if archivo.exists():
        lineas = archivo.read_text(encoding="utf-8").splitlines()[-80:]
        rec = [json.loads(x) for x in lineas if x.strip()]
        if rec:
            df = pd.json_normalize(rec)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("Descargar resultados CSV", df.to_csv(index=False).encode(), "resultados.csv")
    st.caption("Ensayos exploratorios: mantener fija la carga y repetir pruebas para estimar la variabilidad.")


with vista[0]: panel_vivo()
with vista[1]: panel_paralelo()
with vista[2]: panel_modelos()
with vista[3]: panel_anomalias()
with vista[4]: panel_experimentos()
