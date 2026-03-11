FROM apache/airflow:2.7.3

USER root

# Install dependencies + Microsoft ODBC 18
RUN apt-get update && \
    apt-get install -y curl gnupg2 unixodbc unixodbc-dev apt-transport-https && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor \
        | tee /etc/apt/keyrings/microsoft.gpg > /dev/null && \
    echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/opt/mssql-tools18/bin"

USER airflow

RUN pip install --no-cache-dir pyodbc pandas selenium beautifulsoup4 cloudscraper lxml xlrd==2.0.1 google-api-python-client google-auth google-auth-httplib2
