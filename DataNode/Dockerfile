# Use a Python base image with Flask installed
FROM python:3.12

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY DataNode.py requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

# Set any necessary environment variables
ENV FLASK_APP=DataNode.py

# Expose the port specified in the environment variable
EXPOSE ${PORT}

# Command to run your application
CMD ["python3", "DataNode.py"]
