$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Offline full runbook"
Write-Host "Uncomment exactly one command block, then run this file."
Write-Host "Recommended for report: full shortlist = random_forest + gradient_boosting."

# Full: recommended shortlist for report, train + evaluate
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set full --models random_forest gradient_boosting

# Full: all three models, train + evaluate
& $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set full --models logistic_regression gradient_boosting random_forest

# Full: logistic regression + gradient boosting only
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set full --models logistic_regression gradient_boosting

# Full: logistic regression only
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set full --models logistic_regression

# Full: random forest only
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set full --models random_forest

# Full: gradient boosting only
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set full --models gradient_boosting

# Full: train only
# & $PY scripts/offline/run_offline_pipeline.py --phases 3 --feature-set full --models logistic_regression gradient_boosting random_forest

# Full: evaluate only
# & $PY scripts/offline/run_offline_pipeline.py --phases 4 --feature-set full --models logistic_regression gradient_boosting random_forest

# Full: full rerun with data prep
# & $PY scripts/offline/run_offline_pipeline.py --phases 1 2 3 4 --feature-set full --models logistic_regression gradient_boosting random_forest
