# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Flask app files into the container
# This includes app.py, the 'templates' folder, and formatted_chemist_list.csv
COPY . .

# Expose the port that the Flask app will listen on
ENV PORT 8080
EXPOSE 8080

# Command to run the application using Gunicorn (a production-ready WSGI server)
# 'app:app' means: from the 'app.py' file, use the 'app' Flask instance
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 app:app