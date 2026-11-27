#!/bin/bash

set -eou pipefail

cd /app

alembic upgrade head
