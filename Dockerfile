# Databricks Container Services compatible image.
# See: https://docs.databricks.com/en/compute/custom-containers.html
FROM databricksruntime/standard:14.3-LTS

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
COPY pipeline/ /app/pipeline/

# Install project dependencies.  pyspark is already provided by the
# Databricks runtime, so pip will skip or satisfy it automatically.
RUN pip install --no-cache-dir .
COPY generate_bank_data.py /app/

# Do NOT set ENTRYPOINT — Databricks Container Services manages the
# entrypoint to initialize Spark.  spark_python_task provides the script.
# For local testing: docker run <image> python -m pipeline.run_gold /data
