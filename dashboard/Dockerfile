FROM python:3.11-slim
WORKDIR /app
COPY requirements_dashboard.txt .
RUN pip install -r requirements_dashboard.txt
COPY . .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
