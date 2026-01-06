import pandas as pd
from pathlib import Path

# Caminho onde os dados estão salvos
caminho_silver = Path.home() / 'airflow/datalake/silver/caged_processed.parquet'

print("="*50)
print(f"📂 Lendo dados de: {caminho_silver}")
print("="*50)

# 1. Ler o Data Lake INTEIRO
# O Pandas junta todas as pastas (uf=SP, uf=CE, etc) automaticamente
df_total = pd.read_parquet(caminho_silver)

print(f"\n🌎 TOTAL BRASIL:")
print(f"   Linhas: {len(df_total)}")
print(f"   Colunas: {list(df_total.columns)}")
print("\n   Amostra aleatória:")
print(df_total.sample(3))

print("\n" + "="*50)

# 2. Ler APENAS o Ceará (Filtro Inteligente)
# Graças ao particionamento, o Pandas vai ler direto na pasta 'uf=CE'
# sem precisar escanear os outros estados. Isso é performance pura.
print(f"☀️ FILTRANDO SÓ CEARÁ (Fortaleza Representa!):")

df_ce = pd.read_parquet(caminho_silver, filters=[('uf', '==', 'CE')])

print(f"   Linhas do CE: {len(df_ce)}")
print(f"   Média Salarial no CE: R$ {df_ce['salario'].mean():.2f}")
print("\n   Primeiras 5 linhas do CE:")
print(df_ce.head())
print("="*50)
