"""07_gold_mapa_calor.py — job Gold Mapa de Calor: Silver → Gold

Tabelas (nomes de coluna EXATOS — o site lê estes nomes):
  mapa_calor_cliques — uma linha por (pagina, element_id), só eventos com
    element_id e page_location: cliques (eventos), sessoes (sessões distintas
    que clicaram), sessoes_na_pagina (sessões com page_view na página),
    taxa_clique = sessoes ÷ sessoes_na_pagina (nulo se divisor 0),
    intensidade = cliques ÷ maior cliques da mesma página (0 a 1). Taxas a 4 casas.
  mapa_calor_rolagem — uma linha por marco (0, 25, 50, 75, 90) da landing
    (pagina = "/"): entre as sessões com page_view na landing, sessoes_que_chegaram
    (marco 0 = todas; os demais = as que têm scroll na landing com
    percent_scrolled ≥ marco) e pct_sessoes = sessoes_que_chegaram ÷ as do marco 0,
    a 4 casas.

Def 5.4.12 Página = caminho de page_location sem domínio, sem # e sem ?; raiz "/".

Uso:
    docker compose run --rm app python pipeline/07_gold_mapa_calor.py
Pré-requisito: camada Silver existente.
"""

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from comum import (
    caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo,
)

MARCOS_ROLAGEM = [0, 25, 50, 75, 90]


# ── Def 5.4.12: página de uma URL ───────────────────────────────────
def pagina_de(col: str):
    """Caminho de `col` sem domínio, sem # e sem ?; raiz vira '/'."""
    sem_dominio = F.regexp_replace(
        F.regexp_extract(F.col(col), r"^[^?#]*", 0),
        r"^https?://[^/]*", "")
    return F.when(F.length(sem_dominio) == 0, F.lit("/")).otherwise(sem_dominio)


# ── mapa_calor_cliques ─────────────────────────────────────────────
def montar_mapa_cliques(silver: DataFrame) -> DataFrame:
    """Cliques por elemento da página, com taxa e intensidade (R11)."""
    com_elemento = (
        silver
        .filter(F.col("element_id").isNotNull() & F.col("page_location").isNotNull())
        .withColumn("pagina", pagina_de("page_location"))
    )
    cliques_por = com_elemento.groupBy("pagina", "element_id").agg(
        F.count(F.lit(1)).alias("cliques"),
        F.countDistinct("session_id").alias("sessoes"),
    )

    sessoes_na_pagina = (
        silver.filter(F.col("event_name") == "page_view")
        .withColumn("pagina", pagina_de("page_location"))
        .groupBy("pagina")
        .agg(F.countDistinct("session_id").alias("sessoes_na_pagina"))
    )

    w_int = Window.partitionBy("pagina")
    out = (
        cliques_por
        .join(sessoes_na_pagina, "pagina", how="left")
        .withColumn("taxa_clique", F.round(F.try_divide(F.col("sessoes"), F.col("sessoes_na_pagina")), 4))
        .withColumn("_max_cliques", F.max("cliques").over(w_int))
        .withColumn("intensidade", F.round(F.try_divide(F.col("cliques"), F.col("_max_cliques")), 4))
        .select("pagina", "element_id", "cliques", "sessoes",
                "sessoes_na_pagina", "taxa_clique", "intensidade")
        .orderBy("pagina", "element_id")
    )
    return out


# ── mapa_calor_rolagem ────────────────────────────────────────────
def montar_mapa_rolagem(silver: DataFrame) -> DataFrame:
    """Sessões que chegaram a cada marco da landing (R11)."""
    landing = pagina_de("page_location") == F.lit("/")

    page_views = (
        silver.filter((F.col("event_name") == "page_view") & landing)
        .select("session_id").distinct()
    )
    scroll_max = (
        silver.filter((F.col("event_name") == "scroll") & landing)
        .groupBy("session_id")
        .agg(F.max("percent_scrolled").alias("scroll_max"))
    )
    base = page_views.join(scroll_max, "session_id", how="left")

    total_base = base.count()

    def linha(marco):
        if marco == 0:
            chegaram = total_base
        else:
            chegaram = base.filter(F.col("scroll_max") >= marco).count()
        pct = round(chegaram / total_base, 4) if total_base else None
        return (F.lit("/").alias("pagina"), F.lit(marco).alias("marco"),
                F.lit(int(chegaram)).alias("sessoes_que_chegaram"),
                F.lit(pct).alias("pct_sessoes"))

    partes = [silver.sparkSession.range(0, 1).select(*linha(m)) for m in MARCOS_ROLAGEM]
    return reduce(lambda a, b: a.unionByName(b), partes).orderBy("marco")


# ── Job principal ───────────────────────────────────────────────────
def main() -> None:
    spark = criar_sessao("gold")
    exigir_camada(spark, glob_eventos("silver"), "python pipeline/02_silver.py")

    silver = ler_eventos(spark, "silver")

    montar_mapa_cliques(silver).write.mode("overwrite").parquet(caminho("gold/mapa_calor_cliques"))
    montar_mapa_rolagem(silver).write.mode("overwrite").parquet(caminho("gold/mapa_calor_rolagem"))

    titulo("Gold Mapa de Calor")
    spark.stop()


if __name__ == "__main__":
    main()
