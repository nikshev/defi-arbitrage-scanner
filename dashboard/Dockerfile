FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Dashboard-specific deps not in main requirements
COPY dashboard/requirements_dashboard.txt .
RUN pip install --no-cache-dir -r requirements_dashboard.txt

COPY . .

ENV PYTHONPATH=/app

EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.headless=true"]
