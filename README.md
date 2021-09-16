# KYC_pipline
## Know your customer pipeline in apache air flow


For a successful pipeline run take these steps: 
1.	Run you Airflow server
2.	Admin -> connection -> create
3.	Triger the input_dag
4.	Before triggering the File_ process dag, move one of the JSON files into the \tmp folder (for example the request_1411.json). In the program we must give the right name of the JSON file to load. 
5.	Triger the File_process dag 
------------------------------------------------------------------------
### Input: 
For our data, when I read the assignment and what I understood was that I had to create a bunch of dummy data as JSON files with random dates.
This can be done with this main.py (Fourthline\Python codes\Generating data) which I turned into a Dag in Airflow as Input_dag.py.


### Transformation: 
These where the next steps I took for transforming my data:
•	Read JSON file from a file
•	Transform the data into an integer
•	Check the fraud of the request
•	Agent checks the request
•	We create the SQLite database
•	Fill the database with the decision 
•	Create a decision json file with the final output


### Read JSON file: ...
### Check Fraud: ...
### Agent Check: ...
