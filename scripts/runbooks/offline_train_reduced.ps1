$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Offline reduced runbook"
Write-Host "Uncomment exactly one command block, then run this file."
Write-Host "Recommended for report: run all three reduced models in one pass so reduced summaries stay coherent."

# Reduced: all three models, train + evaluate
& $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models logistic_regression gradient_boosting random_forest

# Reduced: logistic regression + gradient boosting only
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models logistic_regression gradient_boosting

# Reduced: logistic regression only
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models logistic_regression

# Reduced: random forest only
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models random_forest

# Reduced: train only
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 --feature-set reduced --models logistic_regression gradient_boosting random_forest

# Reduced: evaluate only
# & $PY scripts/offline/run_offline_pipeline.py --phases 4 --feature-set reduced --models logistic_regression gradient_boosting random_forest

# Reduced: full rerun with data prep
# & $PY scripts/offline/run_offline_pipeline.py --phases 1 2 3 4 --feature-set reduced --models logistic_regression gradient_boosting random_forest
