# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY apply_sp_v3_spill.py sp-batch.py apply_ri.py requirements.txt ./


ENV AWS_ACCESS_KEY_ID=''

ENV AWS_SECRET_ACCESS_KEY=''

ENV AWS_DEFAULT_REGION=us-west-2

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run app.py when the container launches
CMD ["python", "./sp-batch.py"]