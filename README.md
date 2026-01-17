# Industrial Data Foundation

Projeto de referência para implementação de pipelines de dados locais utilizando **Apache Airflow**. Este repositório demonstra um fluxo ETL completo (Extração, Transformação e Carga) simulando o processamento de dados do CAGED (Cadastro Geral de Empregados e Desempregados), com foco em arquitetura auditável, código limpo e boas práticas de Engenharia de Dados.

## 🎯 Contexto e Objetivo

O objetivo deste projeto é fornecer um artefato técnico que demonstre o domínio da orquestração de dados em ambiente local (Linux/WSL). O pipeline resolve o problema clássico de ingestão de dados brutos e sua disponibilização otimizada para análise.

**Destaques Técnicos:**
* Orquestração robusta com Airflow.
* Implementação da arquitetura **Medallion** (Camadas Bronze e Silver).
* Armazenamento otimizado em **Parquet** com particionamento Hive.
* Validação de qualidade de dados via script de auditoria.

## 🏗️ Arquitetura do Pipeline

O fluxo de dados é linear e determinístico, projetado para garantir idempotência e rastreabilidade:

1.  **Ingestão (Extract):** Geração controlada de dados sintéticos simulando a fonte oficial do CAGED. Os dados brutos são persistidos na camada **Bronze** em formato `.csv` (Raw).
2.  **Processamento (Transform):** Leitura da camada Bronze, aplicação de tipagem forte (Casting), limpeza de dados (remoção de outliers e registros inconsistentes) e transformação para formato colunar.
3.  **Armazenamento (Load):** Escrita na camada **Silver** em formato **Parquet**, utilizando compressão Snappy e particionamento físico por Estado (UF). Isso habilita o *partition pruning* em leituras futuras.
4.  **Auditoria (Quality Check):** Task final que valida a existência física dos arquivos, integridade das partições e consistência do schema gerado.

## 🛠️ Stack Tecnológica

As escolhas tecnológicas priorizam a execução "baterias inclusas" (baixo overhead) com ferramentas padrão de mercado:

*   **Apache Airflow (2.10.x):** Padrão da indústria para orquestração baseada em código (Python).
*   **Python 3.12:** Linguagem core da Engenharia de Dados.
*   **Pandas & PyArrow:** Para manipulação em memória e escrita eficiente de formatos colunares.
*   **Linux (WSL2):** Ambiente nativo de execução do Airflow.

> **Justificativa:** A utilização do Airflow em modo Standalone elimina a necessidade de containers Docker pesados para validação funcional, mantendo a complexidade focada na lógica do pipeline e não na infraestrutura.

## 🚀 Instruções de Execução (Local)

Pré-requisitos: Ambiente Linux (Ubuntu/WSL2) e Python 3 instalados.

### 1. Configuração do Ambiente
```bash
# 1. Clone o repositório
git clone https://github.com/p-esteves/industrial-data-foundation.git
cd industrial-data-foundation

# 2. Crie e ative um ambiente virtual
python3 -m venv venv
source venv/bin/activate

# 3. Instale as dependências
pip install -r requirements.txt
```

### 2. Inicialização do Airflow
Configure o diretório home e inicialize o banco de dados local (SQLite):

```bash
export AIRFLOW_HOME=~/airflow

# Instalação/Inicialização modo Standalone (recomendado para dev)
airflow standalone
```
*O comando acima inicializará o banco, criará um usuário admin e subirá os serviços (Webserver e Scheduler). Anote a senha gerada no terminal.*

### 3. Deploy da DAG
Em um novo terminal (com o venv ativo e AIRFLOW_HOME definido):

```bash
# Crie a pasta de DAGs se não existir
mkdir -p ~/airflow/dags

# Copie a DAG do projeto para o diretório do Airflow
cp dags/caged_etl.py ~/airflow/dags/
```

### 4. Execução do Pipeline
1.  Acesse a interface web em `http://localhost:8080`.
2.  Faça login (usuário `admin` e senha gerada no passo 2).
3.  Localize a DAG `industrial-data-foundation`.
4.  Ative a DAG (toggle switch ON) e clique no botão ▶️ (Trigger DAG).

## 🔎 Validação e Resultados

Após a conclusão da DAG (todas as tasks verdes), execute o script de validação local para auditar o Data Lake gerado:

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
