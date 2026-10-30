"""02_silver.py — job Silver: Bronze → Silver (dado limpo)

R5  Deduplicação por event_id
R6  Extração dos params para colunas tipadas
R7  Validação com motivo de rejeição (6 regras mínimas)
R8  Sessionização própria (gap > 30 min por user_pseudo_id)
R9  Identity stitching (user_pseudo_id → user_id)
R10 Tabela de itens com items[] explodido

Uso:
    docker compose run --rm app python pipeline/02_silver.py
Pré-requisito: camada Bronze existente.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from comum import (
    caminho, criar_sessao, exigir_camada, glob_eventos,
    gravar_eventos, ler_eventos, titulo,
)

# ── Catálogo de eventos (CONTRATO_EVENTO.md) ────────────────────────
EVENTOS_VALIDOS = [
    "page_view", "scroll", "click", "site_search",
    "video_start", "video_progress", "video_complete",
    "file_download",
    "view_item_list", "select_item", "view_item",
    "add_to_cart", "remove_from_cart",
    "view_cart", "begin_checkout", "add_shipping_info",
    "add_payment_info", "purchase", "refund",
    "generate_lead", "sign_up", "login",
]

MARCOS_SCROLL = [25, 50, 75, 90]
MARCOS_VIDEO = [0, 10, 25, 50, 75, 100]
GAP_SESSAO_SEG = 30 * 60  # 30 minutos

# ── R6: chaves de params → (coluna, tipo) ───────────────────────────
PARAMS_TIPADOS = [
    ("page_title", "string"), ("percent_scrolled", "int"),
    ("link_url", "string"), ("link_domain", "string"),
    ("link_text", "string"), ("link_id", "string"),
    ("outbound", "boolean"),
    ("search_term", "string"), ("search_results", "int"),
    ("search_area", "string"),
    ("video_title", "string"), ("video_provider", "string"),
    ("video_duration", "double"), ("video_current_time", "double"),
    ("video_percent", "int"),
    ("file_name", "string"), ("file_extension", "string"),
    ("item_list_id", "string"), ("item_list_name", "string"),
    ("currency", "string"), ("value", "double"),
    ("shipping_tier", "string"), ("shipping", "double"),
    ("payment_type", "string"), ("coupon", "string"),
    ("transaction_id", "string"), ("tax", "double"),
    ("reason", "string"), ("lead_source", "string"),
    ("method", "string"), ("element_id", "string"),
]

# ── Colunas de saída ────────────────────────────────────────────────
_COMUNS = [
    "event_id", "event_name", "event_ts",
    "user_pseudo_id", "user_id",
    "page_location", "page_referrer",
    "traffic_source", "traffic_medium", "traffic_campaign",
    "device_category", "device_os", "device_browser",
    # R6 — params extraídos
    "page_title", "percent_scrolled",
    "link_url", "link_domain", "link_text", "link_id", "outbound",
    "search_term", "search_results", "search_area",
    "video_title", "video_provider", "video_duration",
    "video_current_time", "video_percent",
    "file_name", "file_extension",
    "item_list_id", "item_list_name",
    "currency", "value", "shipping_tier", "shipping",
    "payment_type", "coupon", "transaction_id", "tax",
    "reason", "lead_source", "method", "element_id",
    # proveniência
    "_arquivo_origem", "_ingerido_em",
    # partições
    "year", "month", "day",
]

# Eventos válidos: session_id entra logo após user_id
_i = _COMUNS.index("user_id") + 1
COLUNAS_EVENTOS = _COMUNS[:_i] + ["session_id"] + _COMUNS[_i:]

# Rejeitados: motivo_rejeicao entra antes das partições
_j = _COMUNS.index("year")
COLUNAS_REJEITADOS = _COMUNS[:_j] + ["motivo_rejeicao"] + _COMUNS[_j:]

# Itens
COLUNAS_ITENS = [
    "event_id", "event_name", "event_ts",
    "user_pseudo_id", "user_id", "session_id", "transaction_id",
    "item_id", "item_name", "item_category", "price", "quantity",
    "year", "month", "day",
]


# ── R6: extrair params ──────────────────────────────────────────────
def extrair_params(df: DataFrame) -> DataFrame:
    """Extrai cada chave de params para uma coluna com tipo definido."""
    dtype = dict(df.dtypes).get("params", "")
    is_map = "map" in dtype.lower()

    for coluna, tipo in PARAMS_TIPADOS:
        if is_map:
            raw = F.col("params").getItem(coluna)
        else:
            raw = F.col("params").getField(coluna)
        if tipo == "string":
            df = df.withColumn(coluna, raw)
        else:
            df = df.withColumn(coluna, raw.try_cast(tipo))
    return df


# ── Achatar page / traffic / device ─────────────────────────────────
def achatar_contexto(df: DataFrame) -> DataFrame:
    """Copia campos aninhados para colunas planas."""
    return (
        df
        .withColumn("page_location",    F.col("page.location"))
        .withColumn("page_referrer",     F.col("page.referrer"))
        .withColumn("traffic_source",    F.col("traffic.source"))
        .withColumn("traffic_medium",    F.col("traffic.medium"))
        .withColumn("traffic_campaign",  F.col("traffic.campaign"))
        .withColumn("device_category",   F.col("device.category"))
        .withColumn("device_os",         F.col("device.os"))
        .withColumn("device_browser",    F.col("device.browser"))
    )


# ── R7: validação ────────────────────────────────────────────────────
def validar(df: DataFrame) -> DataFrame:
    """Aplica as 6 regras de validação e marca motivo_rejeicao."""

    # Regras 1–5 (cadeia when: primeiro motivo que casar)
    motivo = (
        F.when(
            F.col("event_name").isNull()
            | ~F.col("event_name").isin(EVENTOS_VALIDOS),
            F.lit("evento fora do catálogo"))
        .when(
            F.col("event_ts").isNull(),
            F.lit("timestamp ausente ou futuro"))
        .when(
            F.col("event_ts") > F.col("_ingerido_em"),
            F.lit("timestamp ausente ou futuro"))
        .when(
            F.col("event_name").isin(["purchase", "refund"])
            & F.col("transaction_id").isNull(),
            F.lit("transação sem id"))
        .when(
            (F.col("value").isNotNull() & (F.col("value") < 0))
            | (F.col("value").isNotNull()
               & F.col("currency").isNotNull()
               & (F.col("currency") != "BRL")),
            F.lit("valor monetário inválido"))
        .when(
            (F.col("event_name") == "scroll")
            & F.col("percent_scrolled").isNotNull()
            & ~F.col("percent_scrolled").isin(MARCOS_SCROLL),
            F.lit("marco fora do padrão"))
        .when(
            F.col("event_name").startswith("video_")
            & F.col("video_percent").isNotNull()
            & ~F.col("video_percent").isin(MARCOS_VIDEO),
            F.lit("marco fora do padrão"))
        .otherwise(F.lit(None))
    )
    df = df.withColumn("motivo_rejeicao", motivo)

    # Regra 6: reembolso sem compra
    tx_validos = [
        r.transaction_id
        for r in df.filter(
            (F.col("event_name") == "purchase")
            & F.col("motivo_rejeicao").isNull()
            & F.col("transaction_id").isNotNull()
        ).select("transaction_id").distinct().collect()
    ]

    if tx_validos:
        regra6 = F.when(
            (F.col("event_name") == "refund")
            & F.col("motivo_rejeicao").isNull()
            & ~F.col("transaction_id").isin(tx_validos),
            F.lit("reembolso sem compra"),
        ).otherwise(F.col("motivo_rejeicao"))
    else:
        regra6 = F.when(
            (F.col("event_name") == "refund")
            & F.col("motivo_rejeicao").isNull(),
            F.lit("reembolso sem compra"),
        ).otherwise(F.col("motivo_rejeicao"))

    return df.withColumn("motivo_rejeicao", regra6)


# ── R9: identity stitching ──────────────────────────────────────────
def identity_stitching(df: DataFrame) -> DataFrame:
    """Todo user_pseudo_id recebe o user_id encontrado em qualquer evento seu."""
    w = (
        Window.partitionBy("user_pseudo_id")
        .orderBy("event_ts")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    return df.withColumn(
        "user_id", F.first("user_id", True).over(w))


# ── R8: sessionização ───────────────────────────────────────────────
def sessionizar(df: DataFrame) -> DataFrame:
    """Nova sessão quando o mesmo user_pseudo_id fica > 30 min sem evento."""
    w_ord = Window.partitionBy("user_pseudo_id").orderBy("event_ts")
    w_cum = (
        Window.partitionBy("user_pseudo_id").orderBy("event_ts")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        df
        .withColumn("_prev_ts", F.lag("event_ts").over(w_ord))
        .withColumn(
            "_gap_seg",
            F.unix_timestamp("event_ts") - F.unix_timestamp("_prev_ts"))
        .withColumn(
            "_nova",
            F.when(
                F.col("_gap_seg").isNull() | (F.col("_gap_seg") > GAP_SESSAO_SEG),
                1,
            ).otherwise(0))
        .withColumn(
            "session_id",
            F.concat_ws(
                "-", F.col("user_pseudo_id"),
                F.sum("_nova").over(w_cum).cast("string")))
        .drop("_prev_ts", "_gap_seg", "_nova")
    )


# ── R10: explodir itens ──────────────────────────────────────────────
def explodir_itens(df: DataFrame) -> DataFrame:
    """items[] explodido — uma linha por item por evento."""
    return (
        df.filter(F.col("items").isNotNull() & (F.size("items") > 0))
        .select(
            "event_id", "event_name", "event_ts",
            "user_pseudo_id", "user_id", "session_id",
            "transaction_id", "year", "month", "day",
            F.explode("items").alias("item"),
        )
        .select(
            "event_id", "event_name", "event_ts",
            "user_pseudo_id", "user_id", "session_id", "transaction_id",
            "year", "month", "day",
            F.col("item.item_id").alias("item_id"),
            F.col("item.item_name").alias("item_name"),
            F.col("item.item_category").alias("item_category"),
            F.col("item.price").alias("price"),
            F.col("item.quantity").alias("quantity"),
        )
    )


# ── Job principal ───────────────────────────────────────────────────
def main() -> None:
    spark = criar_sessao("silver")
    exigir_camada(
        spark, glob_eventos("bronze"),
        "python pipeline/01_bronze_ingestao.py")

    # ── Ler Bronze ────────────────────────────────────────────────
    bronze = ler_eventos(spark, "bronze")
    total_bronze = bronze.count()

    # ── R5: Deduplicação por event_id ─────────────────────────────
    dedup = bronze.dropDuplicates(["event_id"])
    total_dedup = dedup.count()
    print(f"  Bronze: {total_bronze} eventos")
    print(f"  Duplicatas removidas (R5): {total_bronze - total_dedup}")

    # ── Preparar event_ts como timestamp ──────────────────────────
    silver = dedup.withColumn(
        "event_ts", F.col("event_ts").try_cast("timestamp"))

    # ── R6: Extrair params para colunas tipadas ───────────────────
    silver = extrair_params(silver)

    # ── Achatar page / traffic / device ───────────────────────────
    silver = achatar_contexto(silver)

    # ── R7: Validação com motivo de rejeição ──────────────────────
    silver = silver.cache()
    silver = validar(silver)

    rejeitados = silver.filter(F.col("motivo_rejeicao").isNotNull())
    validos = silver.filter(F.col("motivo_rejeicao").isNull())

    for linha in rejeitados.groupBy("motivo_rejeicao").count().collect():
        print(f"  Rejeitados — {linha['motivo_rejeicao']}: {linha['count']}")

    n_validos = validos.count()
    print(f"  Válidos: {n_validos}")

    # ── R9: Identity stitching ────────────────────────────────────
    validos = identity_stitching(validos)

    # ── R8: Sessionização ─────────────────────────────────────────
    validos = sessionizar(validos)

    # ── R10: Itens explodidos ─────────────────────────────────────
    itens = explodir_itens(validos)
    n_itens = itens.count()
    print(f"  Itens explodidos (R10): {n_itens}")

    # ── Gravar Silver: eventos válidos ────────────────────────────
    gravar_eventos(validos.select(*COLUNAS_EVENTOS), "silver", "overwrite")

    # ── Gravar Silver: rejeitados (sem partição) ──────────────────
    (rejeitados.select(*COLUNAS_REJEITADOS)
        .write.mode("overwrite")
        .parquet(caminho("silver/eventos_rejeitados")))

    # ── Gravar Silver: itens (particionado por year/month/day) ───
    (itens.select(*COLUNAS_ITENS)
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(caminho("silver/itens")))

    titulo("Silver")
    print(f"  Eventos válidos gravados: {n_validos}")
    print(f"  Eventos rejeitados gravados: {total_dedup - n_validos}")
    print(f"  Linhas de itens gravadas: {n_itens}")
    spark.stop()


if __name__ == "__main__":
    main()
