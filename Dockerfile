# Databricks Container Services compatible image.
# See: https://docs.databricks.com/en/compute/custom-containers.html
FROM databricksruntime/standard:14.3-LTS

COPY pyproject.toml /app/pyproject.toml
WORKDIR /app

# Install project dependencies.  pyspark is already provided by the
# Databricks runtime, so pip will skip or satisfy it automatically.
RUN pip install --no-cache-dir .

COPY pipeline/ /app/pipeline/
COPY generate_bank_data.py /app/

# Not used by Databricks (spark_python_task provides the entrypoint),
# but allows local testing: docker run <image> pipeline.run_gold /data
ENTRYPOINT ["python", "-m"]
