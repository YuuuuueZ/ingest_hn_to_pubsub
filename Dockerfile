# 使用官方 Python 运行环境
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY main.py .

# Cloud Run Job 直接运行 main.py
ENV PORT=8080
CMD ["functions-framework", "--target=hello_http", "--host=0.0.0.0", "--port=8080"]

