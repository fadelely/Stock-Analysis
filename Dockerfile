# base image 
FROM python:3.11-bullseye

RUN apt-get update && \
    apt-get install -y libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# default shell is sh
RUN apt-get update
RUN apt-get install -y --no-install-recommends openjdk-17-jdk
RUN apt-get clean && rm -rf /var/lib/apt/lists/*


WORKDIR /app

# copy work directory
COPY requirements.txt /app/requirements.txt
# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# run the analysis
#CMD ["python", "src/Analysis.py"]
CMD ["python", "-u", "src/Analysis.py"]