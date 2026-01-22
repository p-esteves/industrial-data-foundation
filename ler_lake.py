import pandas as pd
from pathlib import Path
import os

# Configuração de caminhos (Assumindo estrutura padrão do Airflow local)
# Configuração de caminhos (Compatível com Docker e Local)
DEFAULT_PATH = Path.home() / 'airflow' / 'datalake'
BASE_DIR = Path(os.getenv('AIRFLOW_DATALAKE_PATH', DEFAULT_PATH))
SILVER_DIR = BASE_DIR / 'silver' / 'caged_processed.parquet'

def main():
    print("="*80)
    print(f"🏭 INDUSTRIAL DATA FOUNDATION - VALIDAÇÃO DO DATA LAKE (SILVER)")
    print("="*80)

    if not SILVER_DIR.exists():
        print(f"[ERRO] Diretório não encontrado: {SILVER_DIR}")
        print("Certifique-se de que a DAG já foi executada com sucesso.")
        return

    # 1. Análise Física (Partições)
    print(f"\n📂 ANÁLISE DE ARMAZENAMENTO")
    print(f"Caminho Base: {SILVER_DIR}")
    
    # Listar partições (pastas uf=XX)
    particoes = sorted(list(SILVER_DIR.glob('uf=*')))
    
    if not particoes:
        print("[AVISO] Nenhuma partição encontrada.")
    else:
        print(f"Total de Partições (UFs): {len(particoes)}")
        print("\nDetalhamento por Partição:")
        print(f"{'PARTIÇÃO':<15} | {'ARQUIVOS':<10} | {'TAMANHO (KB)':<15}")
        print("-" * 45)
        
        total_arquivos = 0
        
        for p in particoes:
            arquivos = list(p.glob('*.parquet'))
            qtd_arquivos = len(arquivos)
            tamanho_kb = sum(f.stat().st_size for f in arquivos) / 1024
            
            print(f"{p.name:<15} | {qtd_arquivos:<10} | {tamanho_kb:<15.2f}")
            total_arquivos += qtd_arquivos
            
        print("-" * 45)
        print(f"TOTAL GERAL: {total_arquivos} arquivos Parquet encontrados.")

    # 2. Análise Lógica (Dados)
    print(f"\n🧩 ANÁLISE DE DADOS (SCHEMA E CONTEÚDO)")
    try:
        # Leitura otimizada com PyArrow
        df = pd.read_parquet(SILVER_DIR, engine='pyarrow')
        
        print(f"Dimensões do DataFrame: {df.shape[0]} linhas x {df.shape[1]} colunas")
        
        print("\nSchema Detectado:")
        print(df.dtypes)
        
        print("\nAmostra de Dados (5 linhas):")
        print(df.head())
        
        # Validação simples de estatística
        media_salarial = df['salario'].mean()
        print(f"\n📊 Média Salarial Global: R$ {media_salarial:.2f}")
        
    except Exception as e:
        print(f"[ERRO] Falha ao ler os arquivos Parquet: {e}")

    print("\n" + "="*80)
    print("✅ VALIDAÇÃO CONCLUÍDA")

if __name__ == "__main__":
    main()
