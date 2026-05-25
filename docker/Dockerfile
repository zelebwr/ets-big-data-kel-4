# Simple image for NewsPulse runner + dashboard
FROM python:3.12-slim

# Install Java (for Spark) and required system packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-11-jre-headless curl build-essential git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

WORKDIR /app

# Copy requirements and install
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy project
COPY . /app

# Create logs and data folders
RUN mkdir -p /app/logs /app/dashboard/data /app/lakehouse/lakehouse_data

EXPOSE 5000

# By default start a shell; docker-compose will override with specific commands for services
CMD ["bash"]
