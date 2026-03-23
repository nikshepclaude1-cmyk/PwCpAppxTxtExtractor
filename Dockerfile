FROM python:3.11-slim
RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
WORKDIR .
COPY . .
RUN pip3 install -r requirements.txt
CMD ["python3", "main.py"]
