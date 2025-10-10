# Use the official Apache Airflow image as the base (latest version, Python 3.12)
FROM apache/airflow:slim-latest-python3.13

# Install additional OS packages if needed
USER root
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		vim \
		curl \
		git \
		less \
		nano \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements.txt if you want to install extra Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt || true

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Expose Airflow webserver port
EXPOSE 8080

# Default command (can be overridden in docker-compose or devcontainer.json)
CMD ["airflow", "standalone"]
