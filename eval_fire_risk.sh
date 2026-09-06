#!/usr/bin/env bash
set -eo pipefail

# Usage: ./eval_fire_risk.sh MONTH [OUTPUT_DIR]
month="${1:?Usage: $0 MONTH [OUTPUT_DIR]}"
output_dir="${2:-./wildfire_backtest_output}"

source /home/local/WIN/qsu4/miniconda3/etc/profile.d/conda.sh
conda activate analytics

set -u

run_timestamp="$(date +%Y-%m-%d_%H-%M-%S)"
mkdir -p logs

# DEFAULT HINDCAST RANGE 2001-2020
nohup python ./amazonhydroviewer-backend/init_firerisk.py \
    --surface-model-dir /mnt/vast/prakrut/backup/lis_runs/malaria_amazon/forecast/monthly \
    --hcst-start-year 2001 \
    --hcst-end-year 2020 \
    --month "$month" \
    --fire-risk-method both \
    --output-dir "$output_dir" \
    > "./logs/fire_risk_backtest_${run_timestamp}.log" 2>&1

echo "Finished wildfire backtest for month ${month}"
