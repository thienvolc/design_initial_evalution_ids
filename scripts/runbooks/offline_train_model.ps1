$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Offline single-model runbook"
Write-Host "Uncomment one command below, then run this file."

# Reduced: logistic regression
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models logistic_regression

# Reduced: gradient boosting
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models gradient_boosting

# Reduced: random forest
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models random_forest

# Full: logistic regression
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set full --models logistic_regression

# Full: gradient boosting
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set full --models gradient_boosting

# Full: random forest
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set full --models random_forest

# Include data prep (phases 1 2 3 4) for one model
# & $PY scripts/offline/run_offline_pipeline.py --phases 1 2 3 4 --feature-set reduced --models logistic_regression

# Dry-run example
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models logistic_regression --dry-run
