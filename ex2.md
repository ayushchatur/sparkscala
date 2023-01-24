# Ex 2 Solution

### instruction to running
We will be using my custom container available on docker hub , this container has maven installed in base apache/spark:v3.3.1 container

- make a new folder and clone the repository
- navigate to 
``` cd sparkscala/ ```
- pull docker container
``` docker pull  ayushchatur/spar:latest```
- run the docker command to bring the container up and mount the current directory as $/coderun$
``` docker run -it -u root -v $PWD:/coderun ayushchatur/spar:latest bash ```
  This will land you inside the container. Now follow below steps:
- Navigate to the directory /coderun
``` cd /coderun ```
- Run 
``` mvn clean package ``` 
> This operation might take a while  
> the csv file (data) is available within this repositroy only inside the container
- run the file using the command:
  ``` /opt/spark/bin/spark-submit --class org.example.solution --master local[4] target/sparkscala-1.0-SNAPSHOT-jar-with-dependencies.jar```
- the result file is available in result_csv folder with "part.**.csv" as a file name
