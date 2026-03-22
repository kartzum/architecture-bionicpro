#!/bin/bash

set -e

echo "Waiting Airflow..."
for i in {1..30}; do
  if airflow db check > /dev/null 2>&1; then
    echo "✓ Db Airflow ready (retry $i)"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "✗ Db Airflow not ready after 30 retries"
    exit 1
  fi
  echo "Waiting db Airflow... (retry $i/30)"
  sleep 2
done

echo "Init db Airflow..."
airflow db migrate

echo "Creation user airflow_admin from FAB..."

airflow users create \
    --username airflow_admin \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password airflow_password 2>&1 || echo "User airflow_admin already exists"

echo "Check users..."
airflow users list

echo "✓ Init Airflow done"
