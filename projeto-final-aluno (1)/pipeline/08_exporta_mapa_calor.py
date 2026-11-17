"""08_exporta_mapa_calor.py — job de exportação: Gold → site/mapa_calor.js (R11)

Lê as duas tabelas do mapa de calor (mapa_calor_cliques e mapa_calor_rolagem)
pelo Spark, traz as poucas dezenas de linhas para o Python com collect() e
grava o arquivo site/mapa_calor.js com uma única linha:

    window.MAPA_CALOR = {"gerado_em": "...Z", "cliques": [...], "rolagem": [...]};

Cada linha é um objeto JSON com os nomes de coluna exatos da Gold (R11). O
arquivo é um resultado gerado e não fica no S3: o site o lê do disco.

Uso:
    docker compose run --rm app python pipeline/08_exporta_mapa_calor.py
Pré-requisito: Gold mapa de calor existente (rode 07_gold_mapa_calor.py antes).
"""

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

from comum import (
    arquivo_do_projeto, caminho, criar_sessao, exigir_camada, titulo,
)


def main() -> None:
    spark = criar_sessao("export")

    # As duas tabelas do mapa de calor precisam existir (R2).
    exigir_camada(spark, "gold/mapa_calor_cliques", "python pipeline/07_gold_mapa_calor.py")
    exigir_camada(spark, "gold/mapa_calor_rolagem", "python pipeline/07_gold_mapa_calor.py")

    cliques = [r.asDict() for r in spark.read.parquet(caminho("gold/mapa_calor_cliques")).collect()]
    rolagem = [r.asDict() for r in spark.read.parquet(caminho("gold/mapa_calor_rolagem")).collect()]

    agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    dados = {"gerado_em": agora, "cliques": cliques, "rolagem": rolagem}

    destino = arquivo_do_projeto("site/mapa_calor.js")
    destino.parent.mkdir(parents=True, exist_ok=True)
    texto = json.dumps(dados, ensure_ascii=False, default=float)
    destino.write_text("window.MAPA_CALOR = " + texto + ";\n", encoding="utf-8")

    titulo("Exportação Mapa de Calor")
    print(f"  {len(cliques)} linhas de cliques, {len(rolagem)} linhas de rolagem")
    print(f"  gravado em {destino}")
    spark.stop()


if __name__ == "__main__":
    main()
