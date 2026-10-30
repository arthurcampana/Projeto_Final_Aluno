# Plano de implementação — Gold Funil (`pipeline/03_gold_funil.py`)

Job Gold que lê a Silver e produz `funil_compra` e `funil_lead`, seguindo o padrão
de `01_bronze_ingestao.py` / `02_silver.py` e importando as peças de `comum.py`.
Sem worktree: trabalha-se no próprio repositório. Sem dependência de outras Golds.

---

## Esquema da Silver descoberto (lido no código — não inventado)

`ler_eventos(spark, "silver")` devolve os eventos válidos com estas colunas
(verbatim de `COLUNAS_EVENTOS` em `02_silver.py`):

```
event_id, event_name, event_ts, user_pseudo_id, user_id, session_id,
page_location, page_referrer,
traffic_source, traffic_medium, traffic_campaign,
device_category, device_os, device_browser,
page_title, percent_scrolled,
link_url, link_domain, link_text, link_id, outbound,
search_term, search_results, search_area,
video_title, video_provider, video_duration, video_current_time, video_percent,
file_name, file_extension,
item_list_id, item_list_name,
currency, value, shipping_tier, shipping, payment_type, coupon, transaction_id, tax,
reason, lead_source, method, element_id,
_arquivo_origem, _ingerido_em,
year, month, day
```

Colunas que este job usa de fato:
- **`session_id`** — chave de sessão (calculada em R8; formato `<user_pseudo_id>-<n>`).
- **`event_name`** — nome do evento (catálogo de 20).
- **`event_ts`** — timestamp (já `timestamp` na Silver) — usado para "primeiro evento".
- **`traffic_source`, `traffic_medium`** — canal = `"<source> / <medium>"` do 1º evento.
- **`page_location`** — URL; a página é o caminho sem domínio, sem `#` e sem `?` (def 5.4.12).
- **`percent_scrolled`** — int ∈ {25,50,75,90}; `scroll >= 50` ⇒ `percent_scrolled >= 50`.

`silver/itens` (lida com `spark.read.parquet(caminho("silver/itens"))`) tem
(verbatim de `COLUNAS_ITENS`):

```
event_id, event_name, event_ts, user_pseudo_id, user_id, session_id,
transaction_id, item_id, item_name, item_category, price, quantity,
year, month, day
```

Colunas usadas: **`event_name`** (filtrar `select_item`), **`event_ts`** (ordenar),
**`session_id`** (agrupar), **`item_id`** (o plano). A tabela é particionada por
`year/month/day` mas isso é transparente na leitura.

Observação: o array `items[]` é explodido em `silver/itens` e **não** existe como
coluna nos eventos. Logo o `item_id` do `select_item` (plano da sessão) só vem de
`silver/itens` — é por isso que o job precisa lê-la.

---

## Definições mapeadas para as colunas reais (def 5.4.3 / 5.4.4 / 5.4.12)

- **Canal da sessão** = `concat_ws(" / ", traffic_source, traffic_medium)` do
  **primeiro evento** da sessão (primeiro por `event_ts`). Calculado por
  `row_number().over(Window.partitionBy("session_id").orderBy("event_ts"))` = 1.
- **Plano da sessão** = `item_id` do **primeiro `select_item`** da sessão
  (lido de `silver/itens`, ordenado por `event_ts`); `"(nenhum)"` se não houver
  `select_item` na sessão.
- **Página** (def 5.4.12) = caminho de `page_location` sem domínio, sem `#` e sem `?`;
  raiz vira `/`. Fórmula: `regexp_replace(regexp_extract(page_location, r"^[^?#]*", 0), r"^https?://[^/]*", "")`,
  e se o resultado for vazio ⇒ `"/"`.
- **Funil (def 5.4.4)**: uma sessão conta numa etapa se teve aquele evento em
  **qualquer momento**, sem exigir as etapas anteriores. Logo cada etapa é um
  `max(when(event_name = X, 1, 0))` por sessão (flag 0/1).

## Decisões de projeto (com rationale)

1. **`funil_compra` por plano × canal usa o plano e o canal da SESSÃO (não da compra).**
   A unidade do funil é a sessão (5.4.4 conta sessões; conversão = sessões/sessões);
   o desdobramento por plano × canal particiona as sessões, então plano e canal têm
   que ser atributos da sessão para que as conversões por célula sejam coerentes.
   As definições "plano de uma compra" e "canal de uma compra" (5.4.3) governam as
   tabelas de receita (unidade = compra), **não** este funil. `silver/itens` ainda é
   lida — para o `item_id` do primeiro `select_item` (plano da sessão). O plano da
   compra **não** é calculado aqui.

2. **Formato longo** para as duas tabelas: uma linha por `(escopo, [plano,] canal, etapa)`
   com `sessoes`, `conversao_anterior`, `conversao_topo`. O total é uma linha com
   `escopo="total"` (e `plano`/`canal` = sentinela `"(total)"`); o desdobramento tem
   `escopo="plano_canal"` (compra) ou `escopo="canal"` (lead). Nomes de coluna são
   livres; o conteúdo é o que a correção compara.

3. **`view_item` vira uma linha `etapa="view_item"` extra**, fora da sequência de 7
   passos, com `conversao_anterior = try_divide(sessoes_view_item, sessoes_select_item)`
   (a "taxa à parte") e `conversao_topo = null`. Incluída no total e por célula.

4. **Sem arredondamento** nas conversões do funil (só o mapa de calor exige 4 casas,
   R11). Deixa-se o `double` cru do `try_divide`; a tolerância da correção cobre.

5. **Sentinelas de total**: `TOTAL = "(total)"`, `NENHUM = "(nenhum)"` — strings, para
   nunca ter nulo nas colunas de dimensão.

## Spark 4 / modo ANSI (CRÍTICO)

O projeto usa PySpark 4.2 com modo ANSI ligado: **divisão por zero ERRA** o job em
vez de devolver `null`. Toda conversão cujo divisor pode ser zero (etapa anterior
ou o topo ausentes numa célula) **tem que** usar `F.try_divide(a, b)`, que devolve
`null` quando `b = 0`. Aplicar em todas as `conversao_anterior` e `conversao_topo`.

---

# Itens

- [ ] 1. Criar `pipeline/03_gold_funil.py` — job Gold Funil completo (header, imports, constantes, funções de transformação puras e `main`).
      - Header docstring em português no estilo de `02_silver.py`: título, o que faz (funil_compra e funil_lead a partir da Silver), referências (def 5.4.3/5.4.4/5.4.12), bloco `Uso:` com `docker compose run --rm app python pipeline/03_gold_funil.py` e `Pré-requisito: camada Silver existente.`.
      - Imports: `from pyspark.sql import DataFrame, Window`; `from pyspark.sql import functions as F`; `from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo`. (Para unir as linhas longas, `from functools import reduce` ou um loop simples de `unionByName` — fica a critério, mas use `unionByName`.)
      - Constantes: `PASSOS_COMPRA = ["view_item_list","select_item","add_to_cart","view_cart","begin_checkout","add_payment_info","purchase"]`; `PASSOS_LEAD = ["page_view_landing","scroll_50","generate_lead"]`; `TOTAL = "(total)"`; `NENHUM = "(nenhum)"`.
      - Funções puras (recebem DataFrame, devolvem DataFrame/Column — testáveis sem S3):
        - `pagina_de(col: str) -> Column`: expressão da def 5.4.12 — `F.when(F.length(sem_dominio) == 0, F.lit("/")).otherwise(sem_dominio)`, onde `sem_dominio = F.regexp_replace(F.regexp_extract(F.col(col), r"^[^?#]*", 0), r"^https?://[^/]*", "")`. (Confirmado contra `/` , `/checkout` , `/#planos` , `/?utm=x`.)
        - `canal_da_sessao(silver) -> DataFrame`: `session_id` + `canal`. Calcula `row_number().over(Window.partitionBy("session_id").orderBy("event_ts"))`, filtra `rn = 1`, seleciona `session_id` e `F.concat_ws(" / ", F.col("traffic_source"), F.col("traffic_medium")).alias("canal")`. (Assume source/medium não nulos no 1º evento — verdade no gerador.)
        - `plano_da_sessao(itens) -> DataFrame`: `session_id` + `plano`. Filtra `event_name = "select_item"`, `row_number().over(Window.partitionBy("session_id").orderBy("event_ts"))`, filtra `rn = 1`, seleciona `session_id` e `F.col("item_id").alias("plano")`.
        - `flags_funil_compra(silver) -> DataFrame`: `groupBy("session_id").agg(*[F.max(F.when(F.col("event_name") == e, 1).otherwise(0)).alias(e) for e in PASSOS_COMPRA + ["view_item"]])`.
        - `montar_funil_compra(sessoes) -> DataFrame`: recebe o DataFrame por sessão com as flags + `canal` + `plano`. Produz o total (`agg(F.lit(TOTAL).alias("plano"), F.lit(TOTAL).alias("canal"), F.sum(c).alias(c) ...)` sem groupBy) e as células (`groupBy("plano","canal").agg(F.sum(c)...)`); para cada um, monta as conversões por etapa com `F.try_divide`: `conversao_anterior` = `try_divide(<etapa>, <etapa anterior>)` (nulo para `view_item_list`; para a linha `view_item` = `try_divide(view_item, select_item)`); `conversao_topo` = `try_divide(<etapa>, view_item_list)` (=1.0 em `view_item_list`; nulo em `view_item`). Reorganiza em formato longo (uma linha por `etapa`) com colunas `escopo, plano, canal, etapa, sessoes, conversao_anterior, conversao_topo` (`escopo` = `"total"` ou `"plano_canal"`). União total + células via `unionByName`.
        - `flags_funil_lead(silver) -> DataFrame`: adiciona a coluna `pagina = pagina_de("page_location")` e faz `groupBy("session_id").agg` de: `page_view_landing = max(when(event_name="page_view" & pagina="/", 1, 0))`, `scroll_50 = max(when(event_name="scroll" & percent_scrolled >= 50, 1, 0))`, `generate_lead = max(when(event_name="generate_lead", 1, 0))`.
        - `montar_funil_lead(sessoes) -> DataFrame`: análoga à compra, mas sem `plano` e com `escopo` = `"total"` (canal `"(total)"`) ou `"canal"` (canal real). Conversões: `conversao_anterior` nulo em `page_view_landing`; `scroll_50` = `try_divide(scroll_50, page_view_landing)`; `generate_lead` = `try_divide(generate_lead, scroll_50)`. `conversao_topo` = `try_divide(<etapa>, page_view_landing)` (=1.0 no topo). Colunas finais: `escopo, canal, etapa, sessoes, conversao_anterior, conversao_topo`.
      - `main()`: `spark = criar_sessao("gold")`; `exigir_camada(spark, glob_eventos("silver"), "python pipeline/02_silver.py")`; `silver = ler_eventos(spark, "silver")`; `itens = spark.read.parquet(caminho("silver/itens"))`; `canal = canal_da_sessao(silver)`; `plano = plano_da_sessao(itens)`. Funil compra: `sessoes_c = flags_funil_compra(silver).join(canal, "session_id").join(plano, "session_id").fillna(NENHUM, subset=["plano"])`; `montar_funil_compra(sessoes_c).write.mode("overwrite").parquet(caminho("gold/funil_compra"))`. Funil lead: `sessoes_l = flags_funil_lead(silver).join(canal, "session_id")`; `montar_funil_lead(sessoes_l).write.mode("overwrite").parquet(caminho("gold/funil_lead"))`. `titulo("Gold Funil")`; `spark.stop()`. Bloco `if __name__ == "__main__": main()`.
      - Estilo: comentários curtos em português, nomes em português como em `02_silver.py`; não reescrever SparkSession nem acesso S3 (importar de `comum`).
      Files: pipeline/03_gold_funil.py
      Verify: `docker compose run --rm app python -m py_compile pipeline/03_gold_funil.py` — sai 0 (compila sem erro de sintaxe). (A lógica é verificada no item 2; a integração S3 no item 3.)

- [ ] 2. Adicionar `tests/pipeline/test_gold_funil.py` — teste unitário determinístico das funções de transformação (sem S3, no estilo de `tests/pipeline/test_comum.py`).
      - Carregar o módulo pelo caminho (o nome começa com dígito, então `import` direto não funciona): via `importlib.util.spec_from_file_location("gold_funil_mod", <pipeline>/03_gold_funil.py)` + `module_from_spec` + `exec_module`, usando `PIPELINE = Path(__file__).resolve().parents[2] / "pipeline"` (igual ao `conftest.py`). O `from comum import ...` resolve porque o `conftest.py` já põe `pipeline/` no `sys.path`.
      - `test_pagina_de`: DataFrame com `https://loja.exemplo/`→`/`, `https://loja.exemplo/checkout`→`/checkout`, `https://loja.exemplo/#planos`→`/`, `https://loja.exemplo/?utm=x`→`/`.
      - `test_funil_compra`: construir dois DataFrames pequenos (eventos Silver e itens) com o esquema mínimo (colunas: `event_id, event_name, event_ts, session_id, traffic_source, traffic_medium, page_location, percent_scrolled` para eventos; `event_id, event_name, event_ts, session_id, item_id, item_category` para itens). Cenário:
        - S1 (`google`/`cpc`): page_view, view_item_list, select_item(PLANO-PRO), view_item, add_to_cart, view_cart, begin_checkout, add_payment_info, purchase → atingiu os 7 passos + view_item; canal `google / cpc`, plano `PLANO-PRO`.
        - S2 (`direct`/`none`): page_view, view_item_list, select_item(PLANO-BASICO), add_to_cart → só view_item_list/select_item/add_to_cart; canal `direct / none`, plano `PLANO-BASICO`; sem view_item.
        Chamar `flags_funil_compra` → join `canal_da_sessao` → join `plano_da_sessao` → `fillna(NENHUM)` → `montar_funil_compra`. Asserções (filtrando `escopo="total"`): `view_item_list=2, select_item=2, add_to_cart=2, view_cart=1, begin_checkout=1, add_payment_info=1, purchase=1, view_item=1`; `conversao_topo` de `purchase` = 0.5; `conversao_anterior` de `view_item` = 0.5 (= view_item/select_item = 1/2); `conversao_anterior` de `view_item_list` é nulo. Filtrando `escopo="plano_canal"`: célula `(PLANO-PRO, google / cpc)` tem `purchase=1`; célula `(PLANO-BASICO, direct / none)` tem `purchase=0` e `conversao_anterior` de `begin_checkout` **nulo** (divisor `view_cart=0` → prova o `try_divide`).
      - `test_funil_lead`: construir eventos com `page_location` e `percent_scrolled`. S1 (`google`/`cpc`): page_view em `https://loja.exemplo/`, scroll 50, generate_lead → 1/1/1. S2: page_view em `https://loja.exemplo/checkout` (não landing), sem scroll≥50, sem lead → 0/0/0. S3 (`x`/`y`): page_view em `https://loja.exemplo/`, scroll 25, generate_lead → 1/0/1. Asserções no total: `page_view_landing=2, scroll_50=1, generate_lead=2`; `conversao_anterior` de `scroll_50`=0.5, de `generate_lead`=2.0; `conversao_topo` de `generate_lead`=1.0. Na célula `(x / y)`: `conversao_anterior` de `generate_lead` **nulo** (divisor `scroll_50=0`).
      - Usar o fixture `spark` do `conftest.py` (não criar outro); comparar com `collect()` em listas/dicts. Timestamps distintos por evento para ordem determinística.
      Files: tests/pipeline/test_gold_funil.py
      Verify: `docker compose run --rm app pytest tests/pipeline/test_gold_funil.py -q` — todos os testes passam (3 testes acima). Equivale a `docker compose run --rm app pytest tests/pipeline -q`.

- [ ] 3. Teste de integração ponta a ponta via Docker (confirma a escrita no S3 com `s3a://`).
      - Garantir que a Silver existe (se já existir de rodadas anteriores, pular os 3 primeiros passos). Sequência completa a partir do zero:
        `docker compose up -d floci`
        `docker compose run --rm app python gerador/gerar_eventos.py`
        `docker compose run --rm app python pipeline/01_bronze_ingestao.py`
        `docker compose run --rm app python pipeline/02_silver.py`
        `docker compose run --rm app python pipeline/03_gold_funil.py`
      - Esperado: o último comando imprime o cabeçalho `Gold Funil` e sai 0 (não erro de "camada não existe" nem de `DIVIDE_BY_ZERO`).
      - Leitura de volta para inspeção (o floci já está de pé):
        `docker compose run --rm app python -c "from comum import criar_sessao, caminho; s=criar_sessao('check'); fc=s.read.parquet(caminho('gold/funil_compra')); fl=s.read.parquet(caminho('gold/funil_lead')); print('funil_compra', fc.count()); fc.filter(\"escopo='total'\").orderBy('etapa').show(truncate=False); print('funil_lead', fl.count()); fl.filter(\"escopo='total'\").orderBy('etapa').show(truncate=False); s.stop()"`
      - Esperado: `funil_compra` e `funil_lead` têm > 0 linhas; as linhas `escopo='total'` mostram os 8 passos (compra, 7 + view_item) e os 3 passos (lead) com `sessoes` decrescentes e `conversao_anterior`/`conversao_topo` em [0,1] ou nulo; nenhum erro de divisão por zero.
      Files: (nenhum; só executa o pipeline e lê o S3)
      Verify: comandos acima rodam sem erro e as duas pastas `gold/funil_compra/` e `gold/funil_lead/` existem no bucket com conteúdo coerente.

---

## Notas de escopo (fora deste plano)
- Apenas este job Gold (funil). Os outros 4 (engajamento, receita, ML, mapa de calor) e o script de sequência R3 não estão no escopo.
- Atualização do `pipeline/README.md` não está no escopo deste passo (o README já documenta `python pipeline/<job>.py` genericamente).
- Não há dependência de outras Golds: o job lê só Silver.
