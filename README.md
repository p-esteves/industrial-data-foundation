# 🏭 Industrial Data Foundation (ETL Pipeline)

Pipeline de Engenharia de Dados construído com **Apache Airflow** para processamento de dados trabalhistas (CAGED). O projeto simula um ambiente de Big Data completo, desde a ingestão até a disponibilização em Data Lake particionado.

## 🛠️ Tecnologias Utilizadas
* **Orquestração:** Apache Airflow 2.10 (Standalone)
* **Linguagem:** Python 3.12
* **Processamento:** Pandas & Numpy
* **Storage (Data Lake):**
    * 🥉 **Bronze:** Dados Brutos em CSV
    * 🥈 **Silver:** Dados Processados em Parquet (Particionamento Hive por UF)
* **Infraestrutura:** WSL2 (Ubuntu Linux)

## 🚀 Arquitetura do Pipeline

O fluxo de dados segue a arquitetura Medallion (Bronze/Silver):

1.  **Extract (Ingestão):** Geração e extração de dados brutos simulando a base do CAGED.
2.  **Transform (Processamento):**
    * Limpeza de dados (remoção de ruídos/outliers).
    * Conversão de tipagem (Casting).
    * Gravação em formato colunar **Parquet** com compressão Snappy.
3.  **Load (Armazenamento):** Particionamento físico dos arquivos por Estado (`uf=SP`, `uf=CE`, etc) para otimização de consultas.
4.  **Quality Check:** Validação automática da existência e integridade dos arquivos processados.

## 📂 Estrutura do Projeto

```[text]
industrial-data-foundation/
├── dags/
│   └── caged_etl.py      # Código principal da DAG do Airflow
├── ler_lake.py           # Script de validação e leitura do Data Lake (Pandas)
├── requirements.txt      # Dependências do projeto
└── README.md             # Documentação
```

👣 Como Executar
Clone o repositório:
```[bash]
git clone https://github.com/p-esteves/industrial-data-foundation.git
```

Instale as dependências:
```[bash]
pip install -r requirements.txt
```

Configure o Airflow e mova a DAG para a pasta de dags local.

Execute o script de validação para ler o Data Lake:bash
```[bash]
python ler_lake.py
```
