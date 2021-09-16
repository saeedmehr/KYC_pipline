# KYC_pipline
Know your customer pipeline in apache air flow


For a successful pipeline run take these steps: 
1.	Run you Airflow server
2.	Admin -> connection -> create
3.	Triger the input_dag
4.	Before triggering the File_ process dag, move one of the JSON files into the \tmp folder (for example the request_1411.json). In the program we must give the right name of the JSON file to load. 
5.	Triger the File_process dag 
