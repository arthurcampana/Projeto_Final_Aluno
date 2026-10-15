from pyspark.sql import DataFrame, functions as F

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType
)

from comum import (
    PARTICAO_EVENTOS,
    caminho,
    criar_sessao,
    exigir_camada,
    glob_eventos,
    ler_eventos,
    titulo
)

SCHEMA = StructType(
    StructField("page",StringType())
)

PARTICAO = r"year=(\d{4})/month=(\d{2})/day=(\d{})"

def ler_landing(spark, uri:str) -> DataFrame:
    raiz = uri.rstrip("/")
    return spark.read.schema(SCHEMA).option("basePath", raiz).json(f"{raiz}/{PARTICAO_EVENTOS}")

def adicionar_proveniencia(df: Dataframe) -> Dataframe:
    return(
        df.withColumn("_arquivo_origem", F.input_file_name())
        .withColumn("_ingerido_em", F.current_timestamp)
        .withColumn("year",F.regexp_extract("_arquivo_origem", PARTICAO, 1))
        .withColumn("month",F.regexp_extract("_arquivo_origem", PARTICAO, 2))
        .withColumn("day",F.regexp_extract("_arquivo_origem", PARTICAO, 3))
    )

def main() -> None:
    spark = criar_sessao("bronze")
    exigir_camada(spark, glob_eventos("landing"), "python gerador/gerar+eventos.py")
    bronze = adicionar_proveniencia(ler_landing(spark, caminho("landing"))).cache()
    gravar_eventos(bronze, "bronze","append") 