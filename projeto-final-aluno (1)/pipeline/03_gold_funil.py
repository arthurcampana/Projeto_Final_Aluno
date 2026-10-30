"""03_gold_funil.py — job Gold Funil: Silver → Gold (funil_compra e funil_lead)

Def 5.4.3   Canal e plano de uma sessão: canal = "<source> / <medium>" do
            primeiro evento da sessão; plano = item_id do primeiro select_item
            da sessão ("(nenhum)" se não houver).
Def 5.4.4   Funil: uma sessão conta numa etapa se teve aquele evento em qualquer
            momento, sem exigir as etapas anteriores. Conversão sobre a etapa
            anterior = sessões da etapa ÷ sessões da etapa anterior.
Def 5.4.12  Página = caminho de page_location sem o domínio, sem # e sem ?; a
            raiz vira "/". A landing é a página "/".

Tabelas:
  funil_compra — etapas view_item_list → select_item → add_to_cart → view_cart →
                 begin_checkout → add_payment_info → purchase, com conversão sobre
                 a etapa anterior e sobre o topo; total e por plano × canal.
                 view_item (abrir o detalhe do plano) é opcional e não entra na
                 sequência: aparece como taxa à parte = sessões com view_item ÷
                 sessões com select_item.
  funil_lead   — page_view da landing → scroll ≥ 50% → generate_lead, por canal.

Uso:
    docker compose run --rm app python pipeline/03_gold_funil.py
Pré-requisito: camada Silver existente.

Verificação: py_compile (sintaxe) executado localmente; a checagem completa via
Docker depende da Silver populada e não foi executada neste ambiente.
"""

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from comum import (
    caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo,
)

# ── Sentinelas (strings, para nunca ter nulo nas colunas de dimensão) ──
TOTAL = "(total)"
NENHUM = "(nenhum)"

# ── Sequências do funil (def 5.4.4) ──────────────────────────────────
PASSOS_COMPRA = [
    "view_item_list", "select_item", "add_to_cart", "view_cart",
    "begin_checkout", "add_payment_info", "purchase",
]
PASSOS_LEAD = ["page_view_landing", "scroll_50", "generate_lead"]


# ── Def 5.4.12: página de uma URL ───────────────────────────────────
def pagina_de(col: str):
    """Caminho de `col` sem domínio, sem # e sem ?; raiz vira '/'."""
    sem_dominio = F.regexp_replace(
        F.regexp_extract(F.col(col), r"^[^?#]*", 0),
        r"^https?://[^/]*", "")
    return F.when(F.length(sem_dominio) == 0, F.lit("/")).otherwise(sem_dominio)


# ── Def 5.4.3: canal e plano de uma sessão ──────────────────────────
def canal_da_sessao(silver: DataFrame) -> DataFrame:
    """Canal = '<source> / <medium>' do primeiro evento da sessão."""
    w = Window.partitionBy("session_id").orderBy("event_ts")
    return (
        silver.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "session_id",
            F.concat_ws(" / ", F.col("traffic_source"), F.col("traffic_medium")).alias("canal"),
        )
    )


def plano_da_sessao(itens: DataFrame) -> DataFrame:
    """Plano = item_id do primeiro select_item da sessão ('(nenhum)' se não houver)."""
    w = Window.partitionBy("session_id").orderBy("event_ts")
    return (
        itens.filter(F.col("event_name") == "select_item")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("session_id", F.col("item_id").alias("plano"))
    )


# ── Flags por sessão (uma sessão conta na etapa se teve o evento) ───
def flags_funil_compra(silver: DataFrame) -> DataFrame:
    """Uma linha por sessão com flag 0/1 para cada etapa da compra (+ view_item)."""
    return silver.groupBy("session_id").agg(
        *[
            F.max(F.when(F.col("event_name") == e, 1).otherwise(0)).alias(e)
            for e in PASSOS_COMPRA + ["view_item"]
        ]
    )


def flags_funil_lead(silver: DataFrame) -> DataFrame:
    """Uma linha por sessão com flag 0/1 para landing, scroll ≥ 50 e generate_lead."""
    return (
        silver.withColumn("_pagina", pagina_de("page_location"))
        .groupBy("session_id")
        .agg(
            F.max(
                F.when((F.col("event_name") == "page_view") & (F.col("_pagina") == "/"), 1)
                .otherwise(0)
            ).alias("page_view_landing"),
            F.max(
                F.when((F.col("event_name") == "scroll") & (F.col("percent_scrolled") >= 50), 1)
                .otherwise(0)
            ).alias("scroll_50"),
            F.max(F.when(F.col("event_name") == "generate_lead", 1).otherwise(0)).alias("generate_lead"),
        )
    )


# ── Montagem do funil de compra (formato longo) ─────────────────────
def _linhas_compra(wide: DataFrame, escopo: str) -> DataFrame:
    """Do largo (uma coluna por etapa) para o longo (uma linha por etapa)."""
    partes = []
    for i, passo in enumerate(PASSOS_COMPRA):
        if i == 0:
            anterior = F.lit(None).cast("double")            # sem etapa anterior
        else:
            anterior = F.try_divide(F.col(passo), F.col(PASSOS_COMPRA[i - 1]))
        topo = F.try_divide(F.col(passo), F.col(PASSOS_COMPRA[0]))
        partes.append(
            wide.select(
                F.lit(escopo).alias("escopo"),
                F.col("plano"),
                F.col("canal"),
                F.lit(passo).alias("etapa"),
                F.col(passo).alias("sessoes"),
                anterior.alias("conversao_anterior"),
                topo.alias("conversao_topo"),
            )
        )
    # view_item: taxa à parte = sessões com view_item ÷ sessões com select_item
    partes.append(
        wide.select(
            F.lit(escopo).alias("escopo"),
            F.col("plano"),
            F.col("canal"),
            F.lit("view_item").alias("etapa"),
            F.col("view_item").alias("sessoes"),
            F.try_divide(F.col("view_item"), F.col("select_item")).alias("conversao_anterior"),
            F.lit(None).cast("double").alias("conversao_topo"),
        )
    )
    return reduce(lambda a, b: a.unionByName(b), partes)


def montar_funil_compra(sessoes: DataFrame) -> DataFrame:
    """Total + desdobramento por plano × canal, em formato longo."""
    contagem = PASSOS_COMPRA + ["view_item"]
    total = sessoes.agg(
        F.lit(TOTAL).alias("plano"),
        F.lit(TOTAL).alias("canal"),
        *[F.sum(c).alias(c) for c in contagem],
    )
    celulas = sessoes.groupBy("plano", "canal").agg(*[F.sum(c).alias(c) for c in contagem])
    return _linhas_compra(total, "total").unionByName(_linhas_compra(celulas, "plano_canal"))


# ── Montagem do funil de lead (formato longo) ───────────────────────
def _linhas_lead(wide: DataFrame, escopo: str) -> DataFrame:
    """Do largo para o longo (uma linha por etapa)."""
    partes = []
    for i, passo in enumerate(PASSOS_LEAD):
        if i == 0:
            anterior = F.lit(None).cast("double")            # sem etapa anterior
        else:
            anterior = F.try_divide(F.col(passo), F.col(PASSOS_LEAD[i - 1]))
        topo = F.try_divide(F.col(passo), F.col(PASSOS_LEAD[0]))
        partes.append(
            wide.select(
                F.lit(escopo).alias("escopo"),
                F.col("canal"),
                F.lit(passo).alias("etapa"),
                F.col(passo).alias("sessoes"),
                anterior.alias("conversao_anterior"),
                topo.alias("conversao_topo"),
            )
        )
    return reduce(lambda a, b: a.unionByName(b), partes)


def montar_funil_lead(sessoes: DataFrame) -> DataFrame:
    """Total + desdobramento por canal, em formato longo."""
    total = sessoes.agg(
        F.lit(TOTAL).alias("canal"),
        *[F.sum(c).alias(c) for c in PASSOS_LEAD],
    )
    celulas = sessoes.groupBy("canal").agg(*[F.sum(c).alias(c) for c in PASSOS_LEAD])
    return _linhas_lead(total, "total").unionByName(_linhas_lead(celulas, "canal"))


# ── Job principal ───────────────────────────────────────────────────
def main() -> None:
    spark = criar_sessao("gold")
    exigir_camada(spark, glob_eventos("silver"), "python pipeline/02_silver.py")

    silver = ler_eventos(spark, "silver")
    itens = spark.read.parquet(caminho("silver/itens"))

    canal = canal_da_sessao(silver)
    plano = plano_da_sessao(itens)

    # Funil de compra: uma linha por sessão + canal + plano.
    # left no plano: sessões sem select_item ficam com "(nenhum)" (def 5.4.3).
    sessoes_c = (
        flags_funil_compra(silver)
        .join(canal, "session_id", how="left")
        .join(plano, "session_id", how="left")
        .fillna(NENHUM, subset=["plano"])
    )
    montar_funil_compra(sessoes_c).write.mode("overwrite").parquet(caminho("gold/funil_compra"))

    # Funil de lead: uma linha por sessão + canal.
    sessoes_l = flags_funil_lead(silver).join(canal, "session_id", how="left")
    montar_funil_lead(sessoes_l).write.mode("overwrite").parquet(caminho("gold/funil_lead"))

    titulo("Gold Funil")
    spark.stop()


if __name__ == "__main__":
    main()
