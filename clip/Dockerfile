FROM python:3.13-slim

# Install git dependency
RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*
    
# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Set working directory
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Compile the protobuf definitions
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clip.proto

# Run the gRPC server
CMD ["python", "server.py"]