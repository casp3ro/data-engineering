FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "streamlit>=1.30.0" \
    "duckdb>=0.10.0" \
    "plotly>=5.0.0" \
    "pandas>=2.0.0"

COPY src/interfaces/dashboard/app.py /app/app.py

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
