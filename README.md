# Industrial Data Foundation

Projeto de referência para implementação de pipelines de dados locais utilizando **Apache Airflow**. Este repositório demonstra um fluxo ETL completo (Extração, Transformação e Carga) simulando o processamento de dados do CAGED (Cadastro Geral de Empregados e Desempregados), com foco em arquitetura auditável, código limpo e boas práticas de Engenharia de Dados.

## 🎯 Contexto e Objetivo

O objetivo deste projeto é fornecer um artefato técnico que demonstre o domínio da orquestração de dados em ambiente local (Linux/WSL). O pipeline resolve o problema clássico de ingestão de dados brutos e sua disponibilização otimizada para análise.

**Destaques Técnicos:**
* Orquestração robusta com Airflow.
* Implementação da arquitetura **Medallion** (Camadas Bronze e Silver).
* Armazenamento otimizado em **Parquet** com particionamento Hive.
* Validação de qualidade de dados via script de auditoria.
* **Dockerizado:** Ambiente reprodutível com `docker-compose`.

## 🏗️ Arquitetura do Pipeline

O fluxo de dados é linear e determinístico, projetado para garantir idempotência e rastreabilidade:

1.  **Ingestão (Extract):** Geração controlada de dados sintéticos simulando a fonte oficial do CAGED. Os dados brutos são persistidos na camada **Bronze** em formato `.csv` (Raw).
2.  **Processamento (Transform):** Leitura da camada Bronze, aplicação de tipagem forte (Casting), limpeza de dados (remoção de outliers e registros inconsistentes) e transformação para formato colunar.
3.  **Armazenamento (Load):** Escrita na camada **Silver** em formato **Parquet**, utilizando compressão Snappy e particionamento físico por Estado (UF). Isso habilita o *partition pruning* em leituras futuras.
4.  **Auditoria (Quality Check):** Task final que valida a existência física dos arquivos, integridade das partições e consistência do schema gerado.

## 🛠️ Stack Tecnológica

As escolhas tecnológicas priorizam a execução "baterias inclusas" (baixo overhead) com ferramentas padrão de mercado:

*   **Apache Airflow (2.10.x):** Padrão da indústria para orquestração baseada em código (Python).
*   **Docker & Docker Compose:** Para isolamento e reprodutibilidade do ambiente.
*   **Python 3.12:** Linguagem core da Engenharia de Dados.
*   **Pandas & PyArrow:** Para manipulação em memória e escrita eficiente de formatos colunares.
*   **PostgreSQL:** Banco de metadados do Airflow (no ambiente Docker).

> **Justificativa:** A utilização do Airflow em modo Standalone elimina a necessidade de containers Docker pesados para validação funcional, mantendo a complexidade focada na lógica do pipeline e não na infraestrutura.

## 🚀 Como Executar

### Opção 1: Via Docker (Recomendado)
Ideal para avaliação rápida e limpa, sem instalar dependências no seu sistema.

1.  **Inicie o ambiente:**
    ```bash
    docker-compose up -d
    ```
    *Aguarde alguns instantes até que os serviços (Webserver, Scheduler, Postgres) estejam saudáveis.*

2.  **Acesse a interface:**
    *   URL: `http://localhost:8080`
    *   Login: `admin` / `admin`

3.  **Execute o Pipeline:**
    *   Ative a DAG `industrial-data-foundation` (Toggle ON).
    *   Clique em "Trigger DAG" (▶️).

### Opção 2: Local (Python Nativo)
Recomendado para desenvolvimento se você já possui ambiente Linux/WSL configurado.

1.  **Setup:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    
    export AIRFLOW_HOME=~/airflow
    airflow standalone
    ```

2.  **Deploy:**
    ```bash
    mkdir -p ~/airflow/dags
    cp dags/caged_etl.py ~/airflow/dags/
    ```

## 🔎 Validação e Resultados

Após a conclusão da DAG (todas as tasks verdes), execute o script de validação local para auditar o Data Lake gerado:


**Se rodou via Docker:**
```bash
# Executa o script python usando o ambiente do container
docker-compose run --rm airflow-webserver python ler_lake.py
```

**Se rodou Localmente:**
```bash
python ler_lake.py
```

**Resultado esperado:**
*   Relatório listando as partições criadas (ex: `uf=SP`, `uf=CE`).
*   Contagem de arquivos Parquet e tamanho em disco.
*   Exibição do schema detectado e amostra dos dados.

## ⚠️ Limitações e Assunções

*   **Escopo de Demonstração:** O projeto foca na orquestração e estruturação de dados. A camada *Gold* (agregações de negócio) não foi incluída intencionalmente para manter o escopo focado na fundação dos dados.
*   **Schema Fixo:** Assume-se que a fonte de dados mantém contrato estável. Em produção, seria necessário um *Schema Registry* ou validação de contrato mais robusta.
*   **Armazenamento Local:** O Data Lake reside no filesystem local (`~/airflow/datalake`). Em produção, isso seria substituído por S3, GCS ou Azure Blob Storage alterando apenas a variável `BASE_DIR`.

## 🧪 Testes Automatizados

O projeto inclui uma suíte de testes de simulação que valida a lógica ETL (Extração, Transformação e Carga) isoladamente, sem necessidade de subir toda a infraestrutura do Airflow. Útil para CI/CD ou ambientes de desenvolvimento restritos.

**Para rodar a simulação:**

Linux/Mac:
```bash
python tests/simulate_pipeline.py
```

Windows (Script Automático):
```cmd
tests\run_test.bat
```
