FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

# 单进程运行：采集线程在应用进程内，请勿 --scale 本服务或增加 worker 数，
# 以保证“仅一个采集进程、保持源事务顺序”。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
