#!/usr/bin/env bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Build script for OpenLineage SQL parser.
# It's assumed that it will be run on MacOS
set -e

if [ -x "$(command -v /usr/local/bin/python3.7)" ]; then
  /usr/local/bin/python3.7 -m venv .env
else
  python -m venv .env
fi

source .env/bin/activate
source $HOME/.cargo/env

# Disable incremental compilation, since it causes issues.
export CARGO_INCREMENTAL=0

# Run test if indicated to do so.
if [[ $RUN_TESTS = true ]]; then
  cargo test --no-default-features
fi

# Build release wheels
#cd iface-py
maturin build --universal2 --out target/wheels

echo "Package build, trying to import"
echo "Platform:"
python -c "from distutils import util; print(util.get_platform())"
# Verify that it imports and works properly
python -m pip install openlineage-sql --no-index --find-links target/wheels --force-reinstall
python -c "from openlineage_sql import parse; parse([\"SELECT a from b\"])"
echo "all good"