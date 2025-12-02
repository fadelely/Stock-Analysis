# base image 
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# set the working directory 
WORKDIR /app

# copy work directory
COPY requirements.txt /app/requirements.txt
# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# run the analysis
#CMD ["python", "src/Analysis.py"]
CMD ["python", "-u", "src/Analysis.py"]