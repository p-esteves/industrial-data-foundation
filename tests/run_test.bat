@echo off
echo [TESTE] Iniciando Simulacao...

if not exist venv_test (
    echo [INFO] Criando venv...
    python -m venv venv_test
)

echo [INFO] Instalando dependencias...
venv_test\Scripts\python -m pip install pandas pyarrow numpy

echo [INFO] Rodando script de teste...
venv_test\Scripts\python tests\simulate_pipeline.py

echo.
echo [CONCLUIDO] Se voce viu "SUCESSO TOTAL" acima, seu codigo esta perfeito.
pause
