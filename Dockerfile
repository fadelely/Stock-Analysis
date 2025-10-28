# base image 
FROM python:3.11-slim

# set the working directory 
WORKDIR /app

# copy work directory
COPY . .

# install dependencies
RUN pip install numpy pandas seaborn matplotlib

# run the analysis
CMD ["python", "Analysis.py"]