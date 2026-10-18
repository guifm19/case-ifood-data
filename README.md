# Case Técnico de Data Analysis - iFood

Pipeline de dados ponta a ponta e modelagem analítica desenvolvidos no ecossistema Databricks, cobrindo da ingestão bruta (Medallion Architecture) aos testes de hipótese estatísticos, viabilidade financeira de cupons e segmentação RFV.

---

## 1. Instruções de Execução

Para reproduzir integralmente este projeto de forma independente:

1. **Importação do Notebook:**
   * Acesse o seu workspace no Databricks.
   * Navegue até **Workspace** > **Users** > seu usuário.
   * Clique com o botão direito, selecione **Import** e faça o upload do arquivo `Case iFood - Guilherme F.ipynb`.

2. **Ambiente de Computação Recomendado:**
   * **Databricks Serverless** ou cluster Single Node / Multi Node com **Databricks Runtime 13.3+ LTS** (Spark 3.4+).
   * **Unity Catalog:** Certifique-se de que o catálogo `main` e o esquema `default` estejam disponíveis para criação do volume de dados brutos (`/Volumes/main/default/ifood_raw_data`). Caso execute sem Unity Catalog, o código possui fallback automático para `/tmp/ifood_raw_data`.

3. **Execução:**
   * Abra o notebook importado.
   * Clique em **Run All** (Executar Tudo).
   * O pipeline executa desde a limpeza de tabelas prévias, download e extração dos dados brutos, criação das camadas Delta (Bronze, Silver, Gold), até a renderização dos testes estatísticos, painéis visuais e análises de negócio relevantes para o case.

---

## 2. Decisões Técnicas e Integridade de Dados

### Deduplicação de `order_id`
Durante a auditoria de qualidade na camada Silver, identificou-se que pedidos com o mesmo identificador (order_id) possuíam timestamps pertencentes a meses distintos (Dezembro/2018 e Janeiro/2019):
* **Risco Analítico:** A retenção M+1 avalia a coorte de usuários ativos em Dezembro/2018 que voltaram a comprar em Janeiro/2019. Se a deduplicação priorizasse o registro mais recente (Janeiro), o histórico de Dezembro seria esvaziado artificialmente, inviabilizando a coorte base M0. Se criássemos um surrogate key e não desconsiderássemos os pedidos duplicados, seria observada uma retenção de 100% (todos os comprantes de Dezembro teriam pedido em Janeiro).
* **Resolução Implementada:** Foi utilizada uma Window Function temporal descendente (`rn_desc > 1`) para descartar os duplicados mais recentes. Isso manteve a integridade do volume de M0 e permitiu calcular a taxa de recompra real em Janeiro (M1).

### Resiliência e Data Quality
* **Ingestão com Retry:** Mecanismo de reexecução exponencial para mitigar falhas de rede no download das fontes S3 (camada Raw).
* **Tabela de Observabilidade:** Gravação automática de métricas de qualidade na tabela `dq_ingestion_observability` a cada batch executado (monitoramento de IDs nulos, duplicatas e valores negativos).

---

## 3. Arquitetura dos Dados (Medallion)

[S3 Sources: JSON / CSV / TAR.GZ]
                │
                ▼ (Download com Retry + Unpack em Volume)
       ┌─────────────────┐
       │  Bronze Tables  │  -> Append-only com metadados da ingestão (_batch_id, _ingested_at)
       └────────┬────────┘
                │
                ▼ (Deduplicação temporal, tipagem e DQ Checks)
       ┌─────────────────┐
       │  Silver Tables  │  -> Limpeza, conformidade de tipos, drop de duplicatas
       └────────┬────────┘
                │
                ▼ (Modelagem dimensional, métricas LTV e de parceiros)
       ┌─────────────────┐
       │   Gold Tables   │  -> gold_dim_consumers, gold_dim_restaurants, gold_fct_orders
       └────────┬────────┘
                │
                ▼ (Materialização analítica)
       ┌─────────────────┐
       │  Analytics
