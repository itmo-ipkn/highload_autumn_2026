#!/bin/bash

set -eou pipefail

cd /app

exec python3 main_producer.py
