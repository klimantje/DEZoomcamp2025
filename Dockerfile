FROM python:3.9

COPY requirements.txt .
COPY week_1/pipeline.py pipeline.py
RUN pip install -r requirements.txt
# ENTRYPOINT ["python", "pipeline.py"]

