# Dockerfile

# Use the official Python image as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


# Copy the current directory contents into the container at /app
COPY . .

# Ensure the data directory exists
RUN mkdir -p /app/data

# Expose the port
EXPOSE 8000

# Command to run the FastAPI application within the container
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]