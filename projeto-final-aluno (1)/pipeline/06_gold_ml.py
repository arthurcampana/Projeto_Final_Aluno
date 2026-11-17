"""06_gold_ml.py — job Gold ML: Silver → Gold

Tabelas:
  propensao_sessao    — probabilidade de compra por sessão (classificação),
                         uma linha por sessão.
  metricas_modelo     — AUC e F1 no conjunto de teste, mais precisão e recall
                         (classe positiva), e a lista de features usadas.
  segmentos_visitantes — grupo de cada visitante (K-Means) e perfil por grupo.

Def 5.4.6  Propensão: uma linha por sessão (todas as Silver); rótulo verdadeiro
           quando a sessão contém um purchase.
Restrição   Features só do comportamento ANTES do primeiro add_to_cart da
           sessão: rolagem, vídeo, planos vistos, plano selecionado, detalhe
           aberto, buscas, downloads, cliques, canal e dispositivo. Qualquer
           evento do checkout (view_cart em diante) é vazamento e é proibido.
Def 5.4.11 F1, precisão e recall referem-se à classe positiva (sessão com compra).

Uso:
    docker compose run --rm app python pipeline/06_gold_ml.py
Pré-requisito: camada Silver existente.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler

SEED = 42
K_SEGMENTOS = 4

# Features do modelo de propensão (todas anteriores ao primeiro add_to_cart)
FEATURES_NUMERICAS = [
    "scroll_max", "video_percent_max", "viu_planos", "selecionou_plano",
    "abriu_detalhe", "buscas", "downloads", "cliques",
]
FEATURES_CATEGORICAS = ["canal", "dispositivo"]
FEATURES_USADAS = FEATURES_NUMERICAS + FEATURES_CATEGORICAS


# ── Canal e dispositivo do primeiro evento da sessão ────────────────
def _atributos_sessao(silver: DataFrame) -> DataFrame:
    w = Window.partitionBy("session_id").orderBy("event_ts")
    return (
        silver.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "session_id",
            F.concat_ws(" / ", F.col("traffic_source"), F.col("traffic_medium")).alias("canal"),
            F.col("device_category").alias("dispositivo"),
        )
    )


# ── Features de propensão: só o que aconteceu ANTES do 1º add_to_cart ─
def features_propensao(silver: DataFrame) -> DataFrame:
    """Uma linha por sessão com as features pré-carrinho e o rótulo (comprou)."""
    cutoff = (
        silver.filter(F.col("event_name") == "add_to_cart")
        .groupBy("session_id")
        .agg(F.min("event_ts").alias("_cutoff"))
    )
    antes = (
        silver.join(cutoff, "session_id", how="left")
        .filter(F.col("_cutoff").isNull() | (F.col("event_ts") < F.col("_cutoff")))
    )
    feats = antes.groupBy("session_id").agg(
        F.max("percent_scrolled").alias("scroll_max"),
        F.max("video_percent").alias("video_percent_max"),
        F.max(F.when(F.col("event_name") == "view_item_list", 1).otherwise(0)).alias("viu_planos"),
        F.max(F.when(F.col("event_name") == "select_item", 1).otherwise(0)).alias("selecionou_plano"),
        F.max(F.when(F.col("event_name") == "view_item", 1).otherwise(0)).alias("abriu_detalhe"),
        F.sum(F.when(F.col("event_name") == "site_search", 1).otherwise(0)).alias("buscas"),
        F.sum(F.when(F.col("event_name") == "file_download", 1).otherwise(0)).alias("downloads"),
        F.sum(F.when(F.col("event_name") == "click", 1).otherwise(0)).alias("cliques"),
    )
    rotulo = silver.groupBy("session_id").agg(
        F.max(F.when(F.col("event_name") == "purchase", 1).otherwise(0)).alias("comprou"),
    )
    out = (
        feats.join(_atributos_sessao(silver), "session_id", how="left")
        .join(rotulo, "session_id", how="left")
        .fillna(0, subset=FEATURES_NUMERICAS)
        .fillna("(desconhecido)", subset=FEATURES_CATEGORICAS)
    )
    return out


# ── propensao_sessao + metricas_modelo ─────────────────────────────
def _prob_classe1(coluna="probability"):
    """Extrai a probabilidade da classe 1 (compra) do vetor do modelo."""
    return F.udf(lambda v: float(v[1]) if v is not None else None, DoubleType())(F.col(coluna))


def montar_propensao_e_metricas(silver: DataFrame):
    """Treina RandomForest sem vazamento; devolve (propensao_df, metricas_df)."""
    dados = features_propensao(silver).cache()

    idx_canal = StringIndexer(inputCol="canal", outputCol="canal_idx", handleInvalid="keep")
    idx_disp = StringIndexer(inputCol="dispositivo", outputCol="disp_idx", handleInvalid="keep")
    ohe_canal = OneHotEncoder(inputCol="canal_idx", outputCol="canal_ohe")
    ohe_disp = OneHotEncoder(inputCol="disp_idx", outputCol="disp_ohe")
    assembler = VectorAssembler(
        inputCols=FEATURES_NUMERICAS + ["canal_ohe", "disp_ohe"],
        outputCol="features",
    )

    # Peso de classe para o desbalanceamento (compradores são ~6% das sessões)
    n_pos = dados.filter("comprou = 1").count()
    n_neg = dados.filter("comprou = 0").count()
    peso_pos = (n_neg / n_pos) if n_pos else 1.0
    dados = dados.withColumn("peso", F.when(F.col("comprou") == 1, F.lit(float(peso_pos))).otherwise(F.lit(1.0)))

    rf = RandomForestClassifier(
        featuresCol="features", labelCol="comprou", weightCol="peso",
        predictionCol="pred", probabilityCol="probability",
        numTrees=50, maxDepth=6, seed=SEED,
    )
    pipeline = Pipeline(stages=[idx_canal, idx_disp, ohe_canal, ohe_disp, assembler, rf])

    treino, teste = dados.randomSplit([0.8, 0.2], seed=SEED)
    modelo = pipeline.fit(treino)

    # Métricas no conjunto de teste (classe positiva = compra)
    pred_teste = modelo.transform(teste)
    auc = BinaryClassificationEvaluator(labelCol="comprou", metricName="areaUnderROC").evaluate(pred_teste)
    f1 = MulticlassClassificationEvaluator(labelCol="comprou", predictionCol="pred",
                                           metricName="fMeasureByLabel", metricLabel=1.0, beta=1.0).evaluate(pred_teste)
    precisao = MulticlassClassificationEvaluator(labelCol="comprou", predictionCol="pred",
                                                  metricName="precisionByLabel", metricLabel=1.0).evaluate(pred_teste)
    recall = MulticlassClassificationEvaluator(labelCol="comprou", predictionCol="pred",
                                                metricName="recallByLabel", metricLabel=1.0).evaluate(pred_teste)

    metricas = dados.sparkSession.createDataFrame(
        [(round(float(auc), 4), round(float(f1), 4), round(float(precisao), 4),
          round(float(recall), 4), FEATURES_USADAS)],
        schema="auc double, f1 double, precisao double, recall double, features array<string>",
    )

    # Propensão: uma linha por sessão (modelo aplicado a todas)
    propensao = (
        modelo.transform(dados)
        .select("session_id", _prob_classe1().alias("probabilidade"), F.col("comprou"))
        .withColumn("probabilidade", F.round("probabilidade", 4))
    )
    return propensao, metricas


# ── segmentos_visitantes: K-Means por visitante ────────────────────
def montar_segmentos(silver: DataFrame) -> DataFrame:
    """Clusteriza visitantes pelo comportamento; uma linha por visitante com o grupo."""
    feats = (
        silver.groupBy("user_pseudo_id")
        .agg(
            F.countDistinct("session_id").alias("num_sessoes"),
            F.count(F.lit(1)).alias("total_eventos"),
            F.max("percent_scrolled").alias("scroll_max"),
            F.max("video_percent").alias("video_max"),
            F.max(F.when(F.col("event_name") == "generate_lead", 1).otherwise(0)).alias("fez_lead"),
            F.max(F.when(F.col("event_name") == "purchase", 1).otherwise(0)).alias("comprou"),
        )
        .fillna(0, subset=["num_sessoes", "total_eventos", "scroll_max",
                            "video_max", "fez_lead", "comprou"])
    )

    assembler = VectorAssembler(
        inputCols=["num_sessoes", "total_eventos", "scroll_max", "video_max", "fez_lead", "comprou"],
        outputCol="features_raw",
    )
    scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                             withMean=True, withStd=True)
    kmeans = KMeans(featuresCol="features", predictionCol="grupo", k=K_SEGMENTOS, seed=SEED)
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    modelo = pipeline.fit(feats)

    return (
        modelo.transform(feats)
        .select("user_pseudo_id", "grupo", "num_sessoes", "total_eventos",
                "scroll_max", "video_max", "fez_lead", "comprou")
    )


# ── Job principal ───────────────────────────────────────────────────
def main() -> None:
    spark = criar_sessao("gold")
    exigir_camada(spark, glob_eventos("silver"), "python pipeline/02_silver.py")

    silver = ler_eventos(spark, "silver")

    propensao, metricas = montar_propensao_e_metricas(silver)
    propensao.write.mode("overwrite").parquet(caminho("gold/propensao_sessao"))
    metricas.write.mode("overwrite").parquet(caminho("gold/metricas_modelo"))

    montar_segmentos(silver).write.mode("overwrite").parquet(caminho("gold/segmentos_visitantes"))

    titulo("Gold ML")
    spark.stop()


if __name__ == "__main__":
    main()
