# base image 
FROM python:3.11-slim

# set the working directory 
WORKDIR /app

# copy work directory
COPY . .

# install dependencies
RUN pip install numpy pandas seaborn matplotlib

# make port 80 available ? remove??
# EXPOSE 80

# run the analysis
CMD ["python", "Analysis.py"]