# Usamos una imagen oficial de Python
FROM python:3.13.3

# Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos los archivos del proyecto
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Comando por defecto (se puede sobrescribir)
CMD ["python", "app/main.py"]