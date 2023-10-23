#!/usr/bin/env sh

dbt deps
dbt-rpc serve --profiles-dir "." --host "0.0.0.0" --port "8580"
