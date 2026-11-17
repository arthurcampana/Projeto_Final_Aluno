"""rodar_tudo.py — roda os oito jobs em sequência (requisito R3).

Reconstrói o datalake inteiro a partir da landing que já existe: Bronze →
Silver → cinco Golds → exportação do mapa de calor. Não gera dados; apenas
orquestra os jobs, parando no primeiro que falhar.

    docker compose run --rm app python pipeline/rodar_tudo.py

Pré-requisitos: floci no ar e landing populada
(docker compose up -d floci && docker compose run --rm app python gerador/gerar_eventos.py).
"""

import subprocess
import sys
from pathlib import Path

JOBS = [
    "01_bronze_ingestao.py",
    "02_silver.py",
    "03_gold_funil.py",
    "04_gold_engajamento.py",
    "05_gold_receita.py",
    "06_gold_ml.py",
    "07_gold_mapa_calor.py",
    "08_exporta_mapa_calor.py",
]


def main() -> int:
    base = Path(__file__).resolve().parent
    for i, job in enumerate(JOBS, start=1):
        print(f"\n[{i}/{len(JOBS)}] {job}")
        rc = subprocess.run([sys.executable, str(base / job)]).returncode
        if rc != 0:
            print(f"\n✗ {job} falhou (código {rc}). Pipeline interrompido.")
            print("  Corrija o erro e rode de novo; os jobs já concluídos não precisam reprocessar"
                  " (cada um só lê a camada anterior).")
            return rc
        print(f"✓ {job} concluído")
    print("\nPipeline completo: Bronze, Silver, 5 Golds e mapa de calor exportados.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
