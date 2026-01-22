from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Configurar logger
logger = logging.getLogger(__name__)

# Definir diretórios base usando pathlib
import os

# Configurar caminhos via variáveis de ambiente para suportar Docker e Local
# Default (Local): ~/airflow/datalake
# Docker: /opt/airflow/datalake (definido no docker-compose)
DEFAULT_PATH = Path.home() / 'airflow' / 'datalake'
BASE_DIR = Path(os.getenv('AIRFLOW_DATALAKE_PATH', DEFAULT_PATH))
BRONZE_DIR = BASE_DIR / 'bronze'
SILVER_DIR = BASE_DIR / 'silver'

# Argumentos default da DAG
default_args = {
    'owner': 'engenharia-dados',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def extract_caged_data(**context):
    """
    Task 1: Extract - Gera dados fake do CAGED e salva em CSV na camada Bronze.
    """
    try:
        logger.info("Iniciando extração de dados do CAGED...")
        
        # Criar diretório Bronze se não existir
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Diretório Bronze verificado/criado: {BRONZE_DIR}")
        
        # Gerar dados fake simulando CAGED
        np.random.seed(42)
        n_rows = 50000
        
        competencias = pd.date_range(start='2023-01-01', end='2024-12-31', freq='MS')
        ufs = ['SP', 'RJ', 'MG', 'RS', 'BA', 'PR', 'CE', 'PE', 'SC', 'GO']
        cbos = [411005, 411010, 511205, 521110, 782205, 414105, 515105, 422305, 522310, 413205]
        tipos_movimentacao = ['Admissão', 'Desligamento', 'Transferência']
        
        df_fake = pd.DataFrame({
            'competencia': np.random.choice(competencias, n_rows),
            'uf': np.random.choice(ufs, n_rows),
            'cbo': np.random.choice(cbos, n_rows),
            'salario': np.random.uniform(-500, 15000, n_rows),  # Inclui salários negativos propositalmente
            'tipo_movimentacao': np.random.choice(tipos_movimentacao, n_rows)
        })
        
        # Salvar como CSV
        output_file = BRONZE_DIR / 'caged_raw.csv'
        df_fake.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Extração concluída: {n_rows} registros salvos em {output_file}")
        logger.info(f"Primeiras linhas:\n{df_fake.head()}")
        
        # Passar caminho do arquivo para próxima task via XCom
        return str(output_file)
        
    except Exception as e:
        logger.error(f"ERRO na extração: {type(e).__name__} - {str(e)}")
        raise


def transform_caged_data(**context):
    """
    Task 2: Transform - Lê CSV da Bronze, limpa dados e salva como Parquet particionado na Silver.
    """
    try:
        # Verificar se pyarrow está instalado
        try:
            import pyarrow
            logger.info(f"PyArrow detectado: versão {pyarrow.__version__}")
        except ImportError:
            error_msg = "PyArrow não instalado! Execute: pip install pyarrow"
            logger.error(error_msg)
            raise ImportError(error_msg)
        
        logger.info("Iniciando transformação dos dados...")
        
        # Recuperar caminho do arquivo da task anterior
        ti = context['ti']
        bronze_file = ti.xcom_pull(task_ids='extract_caged_data')
        logger.info(f"Lendo arquivo Bronze: {bronze_file}")
        
        # Ler CSV da camada Bronze
        df = pd.read_csv(bronze_file)
        logger.info(f"Total de registros lidos: {len(df)}")
        
        # LIMPEZA: Remover salários negativos
        registros_antes = len(df)
        df = df[df['salario'] >= 0]
        registros_removidos = registros_antes - len(df)
        logger.info(f"Registros com salário negativo removidos: {registros_removidos}")
        
        # TIPAGEM: Converter colunas para tipos adequados
        df['competencia'] = pd.to_datetime(df['competencia'])
        df['uf'] = df['uf'].astype('category')
        df['cbo'] = df['cbo'].astype('int32')
        df['salario'] = df['salario'].astype('float32')
        df['tipo_movimentacao'] = df['tipo_movimentacao'].astype('category')
        
        logger.info(f"Tipos de dados após transformação:\n{df.dtypes}")
        
        # Criar diretório Silver se não existir
        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Diretório Silver verificado/criado: {SILVER_DIR}")
        
        # Salvar como Parquet particionado por UF
        output_path = SILVER_DIR / 'caged_processed.parquet'
        df.to_parquet(
            output_path,
            engine='pyarrow',
            compression='snappy',
            partition_cols=['uf'],
            index=False
        )
        
        logger.info(f"Transformação concluída: {len(df)} registros salvos em {output_path}")
        logger.info(f"Estatísticas do salário: Média={df['salario'].mean():.2f}, Mediana={df['salario'].median():.2f}")
        
        return str(output_path)
        
    except Exception as e:
        logger.error(f"ERRO na transformação: {type(e).__name__} - {str(e)}")
        raise


def quality_check_data(**context):
    """
    Task 3: Quality Check - Verifica se os arquivos Parquet existem e não estão vazios.
    """
    try:
        logger.info("Iniciando verificação de qualidade...")
        
        # Recuperar caminho do Parquet da task anterior
        ti = context['ti']
        silver_path = Path(ti.xcom_pull(task_ids='transform_caged_data'))
        
        # Verificar se o diretório particionado existe
        if not silver_path.exists():
            error_msg = f"FALHA: Arquivo Parquet não encontrado em {silver_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        logger.info(f"✓ Arquivo Parquet encontrado: {silver_path}")
        
        # Listar partições (subpastas uf=XX)
        partitions = list(silver_path.glob('uf=*'))
        if not partitions:
            error_msg = f"FALHA: Nenhuma partição encontrada em {silver_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"✓ Total de partições encontradas: {len(partitions)}")
        logger.info(f"Partições: {[p.name for p in partitions]}")
        
        # Verificar se há arquivos Parquet nas partições
        total_files = 0
        total_size = 0
        for partition in partitions:
            parquet_files = list(partition.glob('*.parquet'))
            total_files += len(parquet_files)
            total_size += sum(f.stat().st_size for f in parquet_files)
        
        if total_files == 0:
            error_msg = f"FALHA: Nenhum arquivo .parquet encontrado nas partições"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"✓ Total de arquivos Parquet: {total_files}")
        logger.info(f"✓ Tamanho total: {total_size / (1024*1024):.2f} MB")
        
        # Ler o Parquet para validar conteúdo
        df_check = pd.read_parquet(silver_path, engine='pyarrow')
        
        if df_check.empty:
            error_msg = "FALHA: DataFrame está vazio após leitura"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"✓ Registros válidos no Parquet: {len(df_check)}")
        logger.info(f"✓ Colunas presentes: {list(df_check.columns)}")
        logger.info("✅ QUALITY CHECK APROVADO: Todos os testes passaram!")
        
        return {
            'status': 'PASSED',
            'total_records': len(df_check),
            'total_partitions': len(partitions),
            'total_files': total_files,
            'size_mb': round(total_size / (1024*1024), 2)
        }
        
    except Exception as e:
        logger.error(f"❌ QUALITY CHECK FALHOU: {type(e).__name__} - {str(e)}")
        raise


# Definir a DAG
with DAG(
    dag_id='industrial-data-foundation',
    default_args=default_args,
    description='Pipeline ETL CAGED - Extração, Transformação e Quality Check',
    schedule='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'caged', 'data-foundation'],
) as dag:
    
    # Task 1: Extract
    task_extract = PythonOperator(
        task_id='extract_caged_data',
        python_callable=extract_caged_data
    )
    
    # Task 2: Transform
    task_transform = PythonOperator(
        task_id='transform_caged_data',
        python_callable=transform_caged_data
    )
    
    # Task 3: Quality Check
    task_quality_check = PythonOperator(
        task_id='quality_check_data',
        python_callable=quality_check_data
    )
    
    # Definir dependências
    task_extract >> task_transform >> task_quality_check
