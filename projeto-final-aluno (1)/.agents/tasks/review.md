# Gold Funil: funil_compra e funil_lead a partir da Silver

O novo job `pipeline/03_gold_funil.py` lê a Silver e produz duas tabelas Gold: `funil_compra` (o funil de compra de 7 etapas mais a taxa à parte do `view_item`, com linha de total e desdobramento por plano × canal) e `funil_lead` (page_view da landing → scroll ≥ 50% → generate_lead, com total e desdobramento por canal). Cada etapa conta uma sessão se ela disparou aquele evento em qualquer momento, sem exigir as etapas anteriores (def 5.4.4), e todas as conversões usam `F.try_divide` para não quebrar com divisor zero sob o modo ANSI do Spark 4. O job reaproveita `comum.py` (sessão, caminhos s3a, leitura de eventos e guarda de camada) e só inicia se a Silver existir.

Watch for: o desdobramento por plano × canal atribui cada sessão pelo canal e pelo plano da própria sessão (primeiro evento / primeiro select_item), não pelo plano da compra — decisão deliberada para que toda sessão, inclusive as que nunca compraram, caibam numa célula, e está documentada no plano. O único ponto real é a verificação: o coder rodou só `py_compile` (sintaxe); a lógica nunca foi executada contra Spark/S3 e nenhum teste foi adicionado.

**Verdict**: APPROVED

## High-level view

O funil de compra marca uma flag 0/1 por sessão para cada etapa (teve o evento em qualquer momento), soma em total e por célula (plano, canal) e reorganiza em formato longo com `conversao_anterior` (÷ etapa anterior) e `conversao_topo` (÷ view_item_list). Como as etapas são contadas de forma independente, o funil não é estritamente monótono: uma etapa posterior pode ter mais sessões que a anterior e uma conversão passar de 1,0 — é o comportamento definido pela def 5.4.4, não um bug. O `view_item` entra como linha extra, fora da sequência, com `conversao_anterior = view_item/select_item`.

O desdobramento usa o canal (source/medium do primeiro evento da sessão por `event_ts`) e o plano (item_id do primeiro `select_item`, "(nenhum)" se não houver), anexados por left join com `fillna` no plano. Isso ignora de propósito a definição de "plano da compra" (o item de categoria "plano" dentro dos itens da compra), que pertence às tabelas de receita: uma sessão que nunca comprou não tem plano de compra, então o primeiro `select_item` é o único atributo que posiciona toda sessão numa célula.

O funil de lead dispara `page_view_landing` só quando o page_view tem `page_location` resolvido para "/" pela função `pagina_de` (caminho sem domínio, `#` e `?`); `scroll_50` exige `percent_scrolled >= 50`; os três passos somam em total e por canal, com as mesmas conversões via `try_divide`.

Cobertura de testes: só `py_compile` (sintaxe) foi rodado; a lógica de transformação e a escrita no S3 nunca foram executadas, e nenhum teste foi criado, embora o plano os listasse. A corretude aqui é confirmada por leitura contra a especificação e por checagem cruzada das colunas com o `02_silver.py`, não por execução.

<details>
<summary>Issues (1)</summary>

1. **Sem verificação por execução** — O job foi só checado por sintaxe (`py_compile`); a lógica do funil e a escrita Parquet em `s3a://` nunca rodaram contra Spark/S3, e os testes unitário e de integração previstos no plano não foram criados. A corretude foi estabelecida por leitura contra a especificação e checagem cruzada dos nomes de coluna com o `02_silver.py`, não por execução. Convém adicionar os testes planejados antes de depender do resultado.

</details>

<details><summary>Details</summary>

### Funil de compra: contagem independente, taxa do view_item e divisão segura

A contagem é por sessão e independente por etapa: `flags_funil_compra` faz `max(when(event_name = e, 1, 0))` por sessão, e `_linhas_compra` monta `conversao_anterior = try_divide(etapa, etapa_anterior)` e `conversao_topo = try_divide(etapa, view_item_list)` por etapa:

```python
F.max(F.when(F.col("event_name") == e, 1).otherwise(0)).alias(e)   # uma sessão conta na etapa se teve o evento
```

Como a contagem é independente, a sequência não é forçada: uma sessão que fez `add_to_cart` sem nunca ter feito `view_item_list` ainda conta em `add_to_cart`. Numa célula (plano, canal) pode haver mais sessões numa etapa posterior que numa anterior, e então `conversao_anterior` passa de 1,0 — é o que a def 5.4.4 manda ("teve aquele evento em qualquer momento, sem exigir as etapas anteriores"), não uma inconsistência. (confirmed)

O `view_item` é tratado à parte: oitava linha com `etapa = "view_item"`, `conversao_anterior = try_divide(view_item, select_item)` (a taxa à parte) e `conversao_topo = null`, somado no total e nas células mas fora da sequência de 7 passos.

Sob ANSI do Spark 4, divisão por zero lança `DIVIDE_BY_ZERO` em vez de devolver null. Toda conversão usa `F.try_divide`, que devolve null com divisor zero — relevante porque uma célula pequena pode ter zero sessões num passo anterior (sessões que chegaram ao `begin_checkout` mas nenhuma passou por `view_cart`).

### Canal e plano como atributos da sessão (não da compra)

O canal vem do primeiro evento da sessão (`row_number` 1 por `event_ts`) e o plano do primeiro `select_item` (`row_number` 1 por `event_ts` em `silver/itens`). Ambos são anexados à tabela de flags por left join:

```
flags_funil_compra(silver)        # session_id + 8 flags
  .join(canal,  "session_id", left)  # + canal (sempre casado)
  .join(plano,  "session_id", left)  # + plano (null p/ sessões sem select_item)
  .fillna("(nenhum)", ["plano"])
```

O canal nunca fica nulo (toda sessão tem um primeiro evento com source/medium, assumidos não nulos no gerador); o plano só fica nulo para sessões sem `select_item`, e o `fillna` as coloca em "(nenhum)".

A escolha deliberada é usar o plano/canal da sessão, não o "plano da compra" (o item de categoria "plano" dentro dos itens da compra). A razão é estrutural: o funil conta sessões em cada etapa, inclusive no topo (`view_item_list`), onde a maioria das sessões ainda não comprou — e uma sessão sem compra não tem "plano de compra". Usar o primeiro `select_item` da sessão é o único atributo que posiciona toda sessão numa célula de forma coerente. O plano da compra governa as tabelas de receita (unidade = compra), não este funil; por isso não é calculado aqui. (confirmed)

### Funil de lead e a landing

`flags_funil_lead` resolve a página de cada evento com `pagina_de` e dispara `page_view_landing` só quando `event_name = "page_view"` e a página é "/" (a landing):

```python
sem_dominio = regexp_replace(regexp_extract(page_location, r"^[^?#]*", 0), r"^https?://[^/]*", "")
pagina = when(length(sem_dominio) == 0, "/").otherwise(sem_dominio)
```

Traçando pela def 5.4.12: `https://loja.exemplo/` → `/`; `…/checkout` → `/checkout`; `…/#planos` → `/` (o `#` corta no `regexp_extract`); `…/?utm=x` → `/` (o `?` corta). A landing é só a raiz. `scroll_50` exige `percent_scrolled >= 50` — na Silver, scroll válido só tem 25/50/75/90 (regra de validação), então equivale a 50, 75 ou 90.

### Cobertura de testes

O cabeçalho do arquivo registra "Verificação: py_compile (sintaxe) executado localmente". Ou seja, só a sintaxe foi checada: a lógica das transformações e a escrita Parquet em `s3a://` nunca rodaram contra Spark/S3, e os testes unitário e de integração previstos no plano (itens 2 e 3) não foram criados. A corretude foi confirmada aqui por leitura contra a especificação e por checagem cruzada dos nomes de coluna com o `02_silver.py` — todas as colunas usadas (`session_id`, `event_name`, `event_ts`, `traffic_source`, `traffic_medium`, `page_location`, `percent_scrolled` nos eventos; `event_name`, `event_ts`, `session_id`, `item_id` em `silver/itens`) existem exatamente com esses nomes na Silver. Resta o risco de runtime não exercitado; rodar os testes planejados cobre essa lacuna. (confirmed)

</details>

<details>
<summary>File map</summary>

- `pipeline/03_gold_funil.py` (novo) — job Gold Funil: lê a Silver, produz `funil_compra` (7 etapas + taxa view_item, total + plano × canal) e `funil_lead` (3 etapas, total + por canal) em Parquet em `s3a://`, com guarda de camada via `exigir_camada`.

Diff completo: `git show dfb290a`.

</details>
