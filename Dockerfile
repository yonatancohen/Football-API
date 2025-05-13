# Use an official Python runtime as a parent image
FROM python:3.11

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /football-api

# Install dependencies
COPY requirements.txt /football-api/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy project
COPY . /football-api/

# Command to run the app using uvicorn
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "main:app", "-b", "0.0.0.0:80", "--workers", "4", "--timeout", "60"]

