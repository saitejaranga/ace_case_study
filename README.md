# ACE Case Study Project

## Overview
This case study covers, preparation of local material, process order data from two different systems called SAP-PRE and SAP-PRD.

Code is developed in modular fashion to have process_order.py and local_material.py with majority of the work maintained in my_utils.py with typing, doc strings and in-line comments on the functionality of the code.

# Instructions to execute the package.

Steps:
1. Navigate to the technical_case_study folder via cmd.
2. Run this command source vevn/bin/activate
3. Run ./run.sh
4. Wait for the program to execute, mean while you can follow the flow in app.log file.

Output:
1. Once the execution is completed, you could view the data in output folder.

# Strucutre:

technical_case_study/
│
├── code/                  
│   ├── schema/
│   └── local_material.py/
|   └── process_order.py/
|   └── main.py/
|   └── my_utils.py/
├── data/   
|   |── system1/
│   └── system2/
├── Flow_Diagrams/                  
│   ├── Related pipeline diagrams/
├── output/                  
│   ├── related data/
├── Other related folders/                  
