import sys
import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

# --- 1. MOCK AIRFLOW ---
# We mock airflow modules BEFORE importing the DAG file so it doesn't crash on missing dependencies
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.DAG'] = MagicMock()
sys.modules['airflow.operators'] = MagicMock()
sys.modules['airflow.operators.python'] = MagicMock()

# --- 2. SETUP ENVIRONMENT ---
# Define a local temporary datalake for testing
TEST_DIR = Path(os.getcwd()) / "test_datalake"
if TEST_DIR.exists():
    shutil.rmtree(TEST_DIR)
TEST_DIR.mkdir()

# Force the DAG to use this test directory
os.environ['AIRFLOW_DATALAKE_PATH'] = str(TEST_DIR)

print(f"🧪 Ambiente de Teste Configurado: {TEST_DIR}")

# --- 3. IMPORT ETL LOGIC ---
# Now we can import the code. We need to append the dags folder to path
sys.path.append(str(Path(os.getcwd()) / "dags"))

try:
    import caged_etl
    print("✅ Importação do código (`caged_etl.py`) realizada com sucesso.")
except ImportError as e:
    print(f"❌ Falha ao importar o código: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Erro de sintaxe ou execução ao importar: {e}")
    sys.exit(1)

# --- 4. EXECUTE PIPELINE ---
# We will simulate the XCom behavior manually

def run_pipeline_simulation():
    print("\n⏯️  Iniciando Simulação do Pipeline ETL...")
    
    # --- TASK 1: EXTRACT ---
    print("\n[1/3] Executando Extração (Extract)...")
    try:
        # Extract returns the path to the Bronze CSV
        bronze_path = caged_etl.extract_caged_data()
        print(f"   -> Sucesso! Arquivo gerado: {bronze_path}")
    except Exception as e:
        print(f"❌ Erro na Extração: {e}")
        return False

    # --- TASK 2: TRANSFORM ---
    print("\n[2/3] Executando Transformação (Transform)...")
    try:
        # Mock Context/XCom for Transform
        # The function expects context['ti'].xcom_pull to return bronze_path
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = bronze_path
        context = {'ti': mock_ti}
        
        silver_path = caged_etl.transform_caged_data(**context)
        print(f"   -> Sucesso! Parquet gerado: {silver_path}")
    except Exception as e:
        print(f"❌ Erro na Transformação: {e}")
        return False

    # --- TASK 3: QUALITY CHECK ---
    print("\n[3/3] Executando Quality Check...")
    try:
        # Mock Context/XCom for Quality Check
        # Expects silver_path
        mock_ti.xcom_pull.return_value = silver_path
        context = {'ti': mock_ti}
        
        result = caged_etl.quality_check_data(**context)
        print(f"   -> Sucesso! Resultado: {result}")
    except Exception as e:
        print(f"❌ Erro no Quality Check: {e}")
        return False
        
    return True

# --- 5. MAIN execution ---
if __name__ == "__main__":
    success = run_pipeline_simulation()
    
    print("\n" + "="*50)
    if success:
        print("🏆 SUCESSO TOTAL! A lógica do código está perfeita.")
        print("   O código Python funciona independentemente do Airflow/Docker.")
    else:
        print("⚠️  Falha na simulação. Verifique os erros acima.")
    print("="*50)
    
    # Cleanup
    # shutil.rmtree(TEST_DIR) # Uncomment to clean up after
