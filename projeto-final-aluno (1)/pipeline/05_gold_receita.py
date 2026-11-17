"""05_gold_receita.py — job Gold Receita e Aquisição: Silver → Gold

Tabelas:
  receita_diaria     — por data da compra, plano e canal: receita bruta,
                       reembolsada e líquida, pedidos, ticket médio.
  taxa_reembolso     — por plano e canal: pedidos com reembolso ÷ pedidos.
  adesao_order_bump  — percentual de compras com BUMP-MENTORIA.
  lead_para_compra   — leads, quantos compraram depois, mediana de dias entre
                       lead e compra; total e por canal do lead.

Def 5.4.2  Receita: bruta = soma de value dos purchase válidos; reembolsada =
           soma de value dos refund válidos atribuída por transaction_id;
           líquida = bruta − reembolsada; ticket médio = bruta ÷ pedidos.
Def 5.4.3  Plano de uma compra = item de categoria "plano" no items do purchase.
           Canal de uma compra = canal da sessão em que ela aconteceu.
Def 5.4.5  Lead → compra: unidade é a pessoa (user_id resolvido; senão
           user_pseudo_id). Primeiro generate_lead da pessoa e primeira compra
           estritamente depois. Dias = (segundos ÷ 86400) fracionário; mediana.
Def 5.4.7  Taxa de reembolso = pedidos com reembolso ÷ pedidos (por plano e canal
           da compra).
Def 5.4.8  Mediana via percentile_approx(dias, 0.5).
Def 5.4.9  Canal de um lead = canal da sessão do primeiro generate_lead da pessoa.

Uso:
    docker compose run --rm app python pipeline/05_gold_receita.py
Pré-requisito: camada Silver existente.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from comum import (
    caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo,
)

TOTAL = "(total)"
NENHUM = "(nenhum)"


# ── Def 5.4.3: canal da sessão (primeiro evento) ────────────────────
def canal_da_sessao(silver: DataFrame) -> DataFrame:
    w = Window.partitionBy("session_id").orderBy("event_ts")
    return (
        silver.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "session_id",
            F.concat_ws(" / ", F.col("traffic_source"), F.col("traffic_medium")).alias("canal"),
        )
    )


# ── Compras com plano (def 5.4.3) e canal da sessão ────────────────
def compras_com_dimensoes(silver: DataFrame, itens: DataFrame) -> DataFrame:
    """Uma linha por purchase: transaction_id, data, value, plano, canal."""
    plano = (
        itens.filter((F.col("event_name") == "purchase") & (F.col("item_category") == "plano"))
        .dropDuplicates(["event_id"])
        .select("event_id", F.col("item_id").alias("plano"))
    )
    compras = (
        silver.filter(F.col("event_name") == "purchase")
        .select("event_id", "session_id", "transaction_id", "event_ts", "value")
        .join(canal_da_sessao(silver), "session_id", how="left")
        .join(plano, "event_id", how="left")
        .fillna(NENHUM, subset=["plano"])
        .withColumn("data_compra", F.to_date("event_ts"))
    )
    return compras


# ── receita_diaria ──────────────────────────────────────────────────
def montar_receita_diaria(silver: DataFrame, itens: DataFrame) -> DataFrame:
    compras = compras_com_dimensoes(silver, itens)

    bruta_ped = compras.groupBy("data_compra", "plano", "canal").agg(
        F.sum("value").alias("receita_bruta"),
        F.countDistinct("transaction_id").alias("pedidos"),
    )

    # Reembolsos atribuídos à compra pelo transaction_id (def 5.4.2)
    refunds = (
        silver.filter(F.col("event_name") == "refund")
        .select("transaction_id", F.col("value").alias("valor_reembolso"))
        .join(
            compras.select("transaction_id", "data_compra", "plano", "canal"),
            "transaction_id", how="left")
    )
    reembolsada = refunds.groupBy("data_compra", "plano", "canal").agg(
        F.sum("valor_reembolso").alias("receita_reembolsada"),
    )

    resumo = (
        bruta_ped.join(reembolsada, ["data_compra", "plano", "canal"], how="left")
        .fillna(0.0, subset=["receita_reembolsada"])
        .withColumn("receita_liquida", F.col("receita_bruta") - F.col("receita_reembolsada"))
        .withColumn("ticket_medio", F.try_divide(F.col("receita_bruta"), F.col("pedidos")))
        .select(
            "data_compra", "plano", "canal",
            "receita_bruta", "receita_reembolsada", "receita_liquida",
            "pedidos", "ticket_medio",
        )
        .orderBy("data_compra", "plano", "canal")
    )
    return resumo


# ── taxa_reembolso ─────────────────────────────────────────────────
def montar_taxa_reembolso(silver: DataFrame, itens: DataFrame) -> DataFrame:
    """Por plano e canal da compra: pedidos com reembolso ÷ pedidos (def 5.4.7)."""
    compras = compras_com_dimensoes(silver, itens).select(
        "transaction_id", "plano", "canal"
    ).dropDuplicates(["transaction_id"])

    refund_tx = (
        silver.filter(F.col("event_name") == "refund")
        .select("transaction_id").dropDuplicates()
        .withColumn("_reembolsado", F.lit(1))
    )
    compras = compras.join(refund_tx, "transaction_id", how="left")

    return (
        compras.groupBy("plano", "canal")
        .agg(
            F.countDistinct("transaction_id").alias("pedidos"),
            F.sum(F.when(F.col("_reembolsado") == 1, 1).otherwise(0)).alias("pedidos_com_reembolso"),
        )
        .withColumn("taxa_reembolso", F.round(F.try_divide(F.col("pedidos_com_reembolso"), F.col("pedidos")), 4))
        .select("plano", "canal", "pedidos", "pedidos_com_reembolso", "taxa_reembolso")
        .orderBy("plano", "canal")
    )


# ── adesao_order_bump ──────────────────────────────────────────────
def montar_adesao_order_bump(itens: DataFrame) -> DataFrame:
    """Percentual de compras com BUMP-MENTORIA (def: order bump)."""
    purchase_items = itens.filter(F.col("event_name") == "purchase").select("event_id", "item_id")
    total = purchase_items.select("event_id").distinct().count()
    com_bump = (
        purchase_items.filter(F.col("item_id") == "BUMP-MENTORIA")
        .select("event_id").distinct().count()
    )
    return (
        itens.sparkSession.range(0, 1)  # um DataFrame de uma linha para carregar os escalares
        .select(
            F.lit(int(total)).alias("total_compras"),
            F.lit(int(com_bump)).alias("compras_com_bump"),
            F.round(F.try_divide(F.lit(float(com_bump)), F.lit(float(total))), 4).alias("pct_adesao"),
        )
    )


# ── lead_para_compra ──────────────────────────────────────────────
def _pessoa(df: DataFrame) -> DataFrame:
    """Acrescenta a coluna 'pessoa' = user_id resolvido, senão user_pseudo_id."""
    return df.withColumn("pessoa", F.coalesce("user_id", "user_pseudo_id"))


def montar_lead_para_compra(silver: DataFrame) -> DataFrame:
    """Leads, quantos compraram depois, mediana de dias (def 5.4.5); total + por canal."""
    canal = canal_da_sessao(silver)

    # Primeiro generate_lead de cada pessoa + canal daquela sessão (def 5.4.9)
    lead_first = (
        _pessoa(silver.filter(F.col("event_name") == "generate_lead"))
        .withColumn("_rn", F.row_number().over(Window.partitionBy("pessoa").orderBy("event_ts")))
        .filter(F.col("_rn") == 1)
        .select("pessoa", F.col("event_ts").alias("lead_ts"), F.col("session_id").alias("lead_session_id"))
        .join(canal, F.col("lead_session_id") == canal.session_id, how="left")
        .drop("session_id")
        .withColumnRenamed("canal", "canal_lead")
        .fillna(NENHUM, subset=["canal_lead"])
    )

    # Primeira compra de cada pessoa
    compra_first = (
        _pessoa(silver.filter(F.col("event_name") == "purchase"))
        .groupBy("pessoa")
        .agg(F.min("event_ts").alias("primeira_compra_ts"))
    )

    por_pessoa = (
        lead_first.join(compra_first, "pessoa", how="left")
        .withColumn(
            "dias",
            F.when(
                F.col("primeira_compra_ts") > F.col("lead_ts"),
                (F.unix_timestamp("primeira_compra_ts") - F.unix_timestamp("lead_ts")) / 86400.0,
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    def _agg(df: DataFrame, escopo: str, canal_col):
        return df.agg(
            F.lit(escopo).alias("escopo"),
            canal_col.alias("canal"),
            F.count(F.lit(1)).alias("leads"),
            F.sum(F.when(F.col("dias").isNotNull(), 1).otherwise(0)).alias("compraram_depois"),
            F.percentile_approx("dias", F.lit(0.5)).alias("mediana_dias"),
        )

    total = _agg(por_pessoa, "total", F.lit(TOTAL))
    por_canal = (
        por_pessoa.groupBy("canal_lead")
        .agg(
            F.count(F.lit(1)).alias("leads"),
            F.sum(F.when(F.col("dias").isNotNull(), 1).otherwise(0)).alias("compraram_depois"),
            F.percentile_approx("dias", F.lit(0.5)).alias("mediana_dias"),
        )
        .withColumn("escopo", F.lit("canal"))
        .withColumnRenamed("canal_lead", "canal")
        .select("escopo", "canal", "leads", "compraram_depois", "mediana_dias")
    )
    return total.unionByName(por_canal)


# ── Job principal ───────────────────────────────────────────────────
def main() -> None:
    spark = criar_sessao("gold")
    exigir_camada(spark, glob_eventos("silver"), "python pipeline/02_silver.py")

    silver = ler_eventos(spark, "silver")
    itens = spark.read.parquet(caminho("silver/itens"))

    montar_receita_diaria(silver, itens).write.mode("overwrite").parquet(caminho("gold/receita_diaria"))
    montar_taxa_reembolso(silver, itens).write.mode("overwrite").parquet(caminho("gold/taxa_reembolso"))
    montar_adesao_order_bump(itens).write.mode("overwrite").parquet(caminho("gold/adesao_order_bump"))
    montar_lead_para_compra(silver).write.mode("overwrite").parquet(caminho("gold/lead_para_compra"))

    titulo("Gold Receita e Aquisição")
    spark.stop()


if __name__ == "__main__":
    main()
