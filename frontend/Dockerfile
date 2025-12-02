# Imagem base do Python
FROM python:3.10-slim

WORKDIR /app

# Instala as bibliotecas
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copia o código do dashboard
COPY . .

# Expõe a porta do Streamlit
EXPOSE 8501

# Comando para rodar (será sobrescrito pelo docker-compose)
CMD ["streamlit", "run", "dashboard_sige.py", "--server.port", "8501", "--server.address", "0.0.0.0"]