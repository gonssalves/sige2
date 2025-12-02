# Imagem base do Python
FROM python:3.10-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de requisitos
COPY requirements.txt .

# Instala as bibliotecas
RUN pip install -r requirements.txt

# Copia o resto do código da API
COPY . .

# Expõe a porta que a API vai usar
EXPOSE 8000

# O comando para rodar a API (será sobrescrito pelo docker-compose)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]