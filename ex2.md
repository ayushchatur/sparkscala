# Ex 2 Solution

### instruction to running
- make a new folder and clone the repository
- navigate to ``` cd openskysol/ ```
- pull docker container
  ``` docker pull  ayushchatur/sparkv3.3.1:latest```
- run the docker command to bring the container up and mount the current directory as $/coderun$
  ``` docker run -v $PWD:/coderun apache/spark:v3.3.1 bash ```
> the csv file (data) is now mapped within $/coderun$ inside the container
- run the file using the command:
  ``` sh ```
- the result file is available in result_csv folder with "part.**.csv" as a file name
