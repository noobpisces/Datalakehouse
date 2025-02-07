FROM python:3.9

# Cài đặt JDK 11 từ mạng (Oracle hoặc OpenJDK)
RUN apt-get update && \
    apt-get install -y wget && \
    wget -q https://download.java.net/openjdk/jdk11/ri/openjdk-11+28_linux-x64_bin.tar.gz -O /tmp/openjdk-11.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    tar -xzf /tmp/openjdk-11.tar.gz -C /usr/lib/jvm && \
    rm /tmp/openjdk-11.tar.gz && \
    rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường JAVA_HOME và PATH
ENV JAVA_HOME=/usr/lib/jvm/jdk-11
ENV PATH=$JAVA_HOME/bin:$PATH

# Cài đặt các package Python yêu cầu
RUN pip install --no-cache-dir \
    notebook \
    jupyterlab \
    findspark \
    pyspark

# Mở cổng 8888 cho Jupyter
EXPOSE 8888

# Lệnh khởi chạy Jupyter Notebook
CMD ["jupyter", "notebook", "--ip='*'", "--port=8888", "--no-browser", "--allow-root"]
