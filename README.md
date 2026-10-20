# Case Técnico de Data Analysis - iFood

Pipeline de dados ponta a ponta e modelagem analítica estratégica, cobrindo desde a ingestão bruta e tratamento na arquitetura Medalhão (Bronze, Silver e Gold) até testes de hipótese estatísticos (Z-Test), viabilidade financeira de cupons e segmentação RFV.

O projeto foi estruturado para ser **agnóstico de plataforma**: pode ser executado interativamente no Databricks ou de forma autônoma como um script Python/PySpark (`main.py`) via terminal, esteiras de CI/CD, contêineres Docker ou orquestradores (ex: Apache Airflow).

---

## 1. Instruções de Execução

Escolha o ambiente de sua preferência para reproduzir integralmente os resultados:

### Opção A: Databricks (Notebook Interativo)
1. **Importação:**
   * Acesse o workspace do Databricks.
   * Navegue até **Workspace** > **Users** > `seu_usuario`.
   * Clique com o botão direito, selecione **Import** e faça o upload do notebook `Case iFood (prod) - Guilherme F.ipynb`.
2. **Cluster / Runtime:**
   * Databricks Runtime 13.3+ LTS (Apache Spark 3.4+).
   * Catálogo `main` e schema `default` disponíveis (o pipeline possui fallback automático para `/tmp/ifood_raw_data` caso não utilize Unity Catalog).
3. **Execução:**
   * Clique em **Run All**. O notebook executará a ingestão, os Data Marts Delta Lake, as tabelas formatadas e os gráficos executivos.

---

### Opção B: Script Python Standalone (`main.py`)
Indicado para execução local ou submissão via `spark-submit`:

1. **Pré-requisitos:**
   * Python 3.9+ instalado.
   * Java JRE/JDK 8, 11 ou 17 configurado no sistema (`java -version`).
2. **Configuração do Ambiente:**
   ```bash
   # Crie e ative um ambiente virtual
   python -m venv venv
   source venv/bin/activate  # No Windows: .\venv\Scripts\activate

   # Instale as dependências
   pip install -r requirements.txt
---

## 2. Decisões Técnicas e Integridade de Dados

### Deduplicação de `order_id`
Durante a auditoria de qualidade na camada Silver, identificou-se que pedidos com o mesmo identificador (order_id) possuíam timestamps pertencentes a meses distintos (Dezembro/2018 e Janeiro/2019):
* **Risco Analítico:** A retenção M+1 avalia a coorte de usuários ativos em Dezembro/2018 que voltaram a comprar em Janeiro/2019. Se a deduplicação priorizasse o registro mais recente (Janeiro), o histórico de Dezembro seria esvaziado artificialmente, inviabilizando a coorte base M0. Se criássemos um surrogate key e não desconsiderássemos os pedidos duplicados, seria observada uma retenção de 100% (todos os comprantes de Dezembro teriam pedido em Janeiro).
* **Resolução Implementada:** Foi utilizada uma Window Function temporal descendente (`rn_desc > 1`) para descartar os duplicados mais recentes. Isso manteve a integridade do volume de M0 e permitiu calcular a taxa de recompra real em Janeiro (M1).

### Marcação de Usuários
Durante a auditoria de qualidade na camada Silver, foram identificados 310 usuários sem marcação de 'target' ou 'controle' (estes customer_id não constavam na tabela de marcação de ab_test_group). Para fins de consistência, estes usuários foram assumidos como usuários 'controle', ou seja, usuários que não tiveram acesso ao cupom do teste A/B.

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
