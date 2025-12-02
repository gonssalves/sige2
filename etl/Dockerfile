FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
# Define o script como o comando padrão
CMD ["python", "etl.py"]