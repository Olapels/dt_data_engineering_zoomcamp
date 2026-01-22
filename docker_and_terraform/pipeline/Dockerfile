# base Docker image
FROM python:3.13.11-slim

# set up image by installing prereqs
RUN pip install pandas pyarrow

#cwd setup inside container
WORKDIR /app
#copy  script into container
COPY pipeline.py pipeline.py

#just run the script copied into the container
ENTRYPOINT ["python", "pipeline.py"]