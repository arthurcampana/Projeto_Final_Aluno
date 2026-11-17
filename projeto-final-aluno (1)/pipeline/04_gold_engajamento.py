"""04_gold_engajamento.py — job Gold Engajamento: Silver → Gold

Tabelas:
  sessoes          — uma linha por sessão: duração, eventos, scroll máximo,
                     maior video_percent, fez lead, comprou, canal, dispositivo.
  retencao_video   — das sessões com video_start, percentual que atingiu
                     10, 25, 50, 75 e 100%.
  buscas_faq       — termo, total de buscas, buscas sem resultado.
  saidas_downloads — cliques de saída por domínio; downloads por arquivo.

Def 5.4.3   Canal da sessão = "<source> / <medium>" do primeiro evento.
Def 5.4.12 Página = caminho de page_location sem domínio, sem # e sem ?; raiz "/".

Uso:
    docker compose run --rm app python pipeline/04_gold_engajamento.py
Pré-requisito: camada Silver existente.
"""

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from comum import (
    caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo,
)

MARCOS_VIDEO = [10, 25, 50, 75, 100]


# ── Def 5.4.12: página de uma URL ───────────────────────────────────
def pagina_de(col: str):
    """Caminho de `col` sem domínio, sem # e sem ?; raiz vira '/'."""
    sem_dominio = F.regexp_replace(
        F.regexp_extract(F.col(col), r"^[^?#]*", 0),
        r"^https?://[^/]*", "")
    return F.when(F.length(sem_dominio) == 0, F.lit("/")).otherwise(sem_dominio)


# ── Def 5.4.3: canal e dispositivo do primeiro evento da sessão ─────
def atributos_primeiro_evento(silver: DataFrame) -> DataFrame:
    """session_id + canal + dispositivo, todos do primeiro evento da sessão."""
    w = Window.partitionBy("session_id").orderBy("event_ts")
    return (
        silver.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "session_id",
            F.concat_ws(" / ", F.col("traffic_source"), F.col("traffic_medium")).alias("canal"),
            F.col("device_category").alias("dispositivo"),
            F.col("device_os").alias("sistema"),
            F.col("device_browser").alias("navegador"),
        )
    )


# ── sessoes: uma linha por sessão ──────────────────────────────────
def montar_sessoes(silver: DataFrame) -> DataFrame:
    """Resumo de cada visita: duração (s), nº de eventos, scroll e vídeo máximos,
    flags de lead/compra, canal e dispositivo."""
    agregados = (
        silver.groupBy("session_id")
        .agg(
            (F.unix_timestamp(F.max("event_ts")) - F.unix_timestamp(F.min("event_ts")))
            .alias("duracao_seg"),
            F.count(F.lit(1)).alias("eventos"),
            F.max("percent_scrolled").alias("scroll_max"),
            F.max("video_percent").alias("video_percent_max"),
            F.max(F.when(F.col("event_name") == "generate_lead", 1).otherwise(0)).alias("fez_lead"),
            F.max(F.when(F.col("event_name") == "purchase", 1).otherwise(0)).alias("comprou"),
        )
    )
    return (
        agregados
        .join(atributos_primeiro_evento(silver), "session_id", how="left")
        .select(
            "session_id", "duracao_seg", "eventos", "scroll_max",
            "video_percent_max", "fez_lead", "comprou", "canal", "dispositivo",
            "sistema", "navegador",
        )
    )


# ── retencao_video: % de quem deu play que chegou a cada marco ──────
def montar_retencao_video(silver: DataFrame) -> DataFrame:
    """Das sessões com video_start, quantas chegaram a 10/25/50/75/100%."""
    video = silver.filter(F.col("event_name").like("video_%"))
    por_sessao = (
        video.groupBy("session_id")
        .agg(
            F.max(F.when(F.col("event_name") == "video_start", 1).otherwise(0)).alias("_start"),
            F.max("video_percent").alias("_vmax"),
        )
        .filter(F.col("_start") == 1)
    )
    contagens = por_sessao.agg(
        F.count(F.lit(1)).alias("denom"),
        *[F.sum(F.when(F.col("_vmax") >= m, 1).otherwise(0)).alias(f"m{m}") for m in MARCOS_VIDEO],
    )
    # formato longo: uma linha por marco
    partes = []
    for m in MARCOS_VIDEO:
        partes.append(
            contagens.select(
                F.lit(m).alias("marco"),
                F.col(f"m{m}").alias("sessoes"),
                F.round(F.try_divide(F.col(f"m{m}"), F.col("denom")), 4).alias("pct"),
            )
        )
    return reduce(lambda a, b: a.unionByName(b), partes)


# ── buscas_faq: termo, total, sem resultado ─────────────────────────
def montar_buscas_faq(silver: DataFrame) -> DataFrame:
    """Termos procurados no FAQ, com total de buscas e buscas sem resultado."""
    return (
        silver.filter(F.col("event_name") == "site_search")
        .groupBy(F.col("search_term").alias("termo"))
        .agg(
            F.count(F.lit(1)).alias("total_buscas"),
            F.sum(F.when(F.col("search_results") == 0, 1).otherwise(0)).alias("buscas_sem_resultado"),
        )
        .orderBy(F.col("total_buscas").desc())
    )


# ── saidas_downloads: saídas por domínio + downloads por arquivo ───
def montar_saidas_downloads(silver: DataFrame) -> DataFrame:
    """Cliques de saída (outbound) por domínio e downloads por arquivo."""
    saidas = (
        silver.filter((F.col("event_name") == "click") & (F.col("outbound") == F.lit(True)))
        .groupBy(F.col("link_domain").alias("destino"))
        .agg(F.count(F.lit(1)).alias("cliques"))
        .withColumn("tipo", F.lit("saida"))
        .select("tipo", "destino", "cliques")
    )
    downloads = (
        silver.filter(F.col("event_name") == "file_download")
        .groupBy(F.col("file_name").alias("destino"))
        .agg(F.count(F.lit(1)).alias("cliques"))
        .withColumn("tipo", F.lit("download"))
        .select("tipo", "destino", "cliques")
    )
    return saidas.unionByName(downloads)


# ── Job principal ───────────────────────────────────────────────────
def main() -> None:
    spark = criar_sessao("gold")
    exigir_camada(spark, glob_eventos("silver"), "python pipeline/02_silver.py")

    silver = ler_eventos(spark, "silver")

    montar_sessoes(silver).write.mode("overwrite").parquet(caminho("gold/sessoes"))
    montar_retencao_video(silver).write.mode("overwrite").parquet(caminho("gold/retencao_video"))
    montar_buscas_faq(silver).write.mode("overwrite").parquet(caminho("gold/buscas_faq"))
    montar_saidas_downloads(silver).write.mode("overwrite").parquet(caminho("gold/saidas_downloads"))

    titulo("Gold Engajamento")
    spark.stop()


if __name__ == "__main__":
    main()
