from __future__ import annotations

import re
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from comum import (
    criar_sessao,
    exigir_camada,
    glob_eventos,
    gravar_eventos,
    ler_eventos,
    titulo,
)

# Schema explícito completo do evento na Landing (Requisito R4)
SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("user_pseudo_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_hint", StringType(), True),
    StructField("page", StructType([
        StructField("location", StringType(), True),
        StructField("referrer", StringType(), True),
    ]), True),
    StructField("traffic", StructType([
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("campaign", StringType(), True),
    ]), True),
    StructField("device", StructType([
        StructField("category", StringType(), True),
        StructField("os", StringType(), True),
        StructField("browser", StringType(), True),
    ]), True),
    StructField("params", StructType([
        StructField("page_title", StringType(), True),
        StructField("percent_scrolled", LongType(), True),
        StructField("link_url", StringType(), True),
        StructField("link_domain", StringType(), True),
        StructField("link_text", StringType(), True),
        StructField("link_id", StringType(), True),
        StructField("outbound", BooleanType(), True),
        StructField("search_term", StringType(), True),
        StructField("search_results", LongType(), True),
        StructField("search_area", StringType(), True),
        StructField("video_title", StringType(), True),
        StructField("video_provider", StringType(), True),
        StructField("video_duration", DoubleType(), True),
        StructField("video_current_time", DoubleType(), True),
        StructField("video_percent", LongType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_extension", StringType(), True),
        StructField("item_list_id", StringType(), True),
        StructField("item_list_name", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("shipping_tier", StringType(), True),
        StructField("shipping", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("coupon", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("tax", DoubleType(), True),
        StructField("reason", StringType(), True),
        StructField("lead_source", StringType(), True),
        StructField("method", StringType(), True),
        StructField("element_id", StringType(), True),
    ]), True),
    StructField("items", ArrayType(
        StructType([
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("item_category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", LongType(), True),
        ])
    ), True),
])

# Regex ajustada para extrair year, month e day do caminho do arquivo
PARTICAO_REGEX = r"year=(\d{4})/month=(\d{2})/day=(\d{2})"


def adicionar_proveniencia(df: DataFrame) -> DataFrame:
    """Adiciona as colunas de linhagem e extrai as partições de data a partir do nome do arquivo."""
    return (
        df.withColumn("_arquivo_origem", F.input_file_name())
        .withColumn("_ingerido_em", F.current_timestamp())
        .withColumn("year", F.regexp_extract("_arquivo_origem", PARTICAO_REGEX, 1))
        .withColumn("month", F.regexp_extract("_arquivo_origem", PARTICAO_REGEX, 2))
        .withColumn("day", F.regexp_extract("_arquivo_origem", PARTICAO_REGEX, 3))
    )


def main() -> None:
    titulo("Bronze")
    spark = criar_sessao("bronze")

    # Garante que a camada anterior (landing) existe antes de rodar (R2)
    exigir_camada(spark, glob_eventos("landing"), "python gerador/gerar_eventos.py")

    # Lê a landing usando ler_eventos do comum.py com o schema explícito (R4)
    landing = ler_eventos(spark, "landing", formato="json", schema=SCHEMA)

    # Aplica proveniência
    bronze = adicionar_proveniencia(landing)

    # Grava a Bronze em Parquet no modo append (R4)
    gravar_eventos(bronze, "bronze", modo="append")

    spark.stop()


if __name__ == "__main__":
    main()