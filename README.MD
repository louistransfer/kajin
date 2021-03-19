# Kajin, a Jinka wrapper

## Overview

Kajin is a wrapper for the Jinka API ([https://www.jinka.fr/]). It directly authentificates with a Jinka account and grabs the list of appartments found on configured alerts, then returns a dataset as a CSV and an Excel file. The dataset contains various pieces of data such as the area, the rent or the price per square meter.

This project was created during a period of strong demand on Paris for flats, which made navigation difficult on the website. This app aims to tackle this issue by generating a dataset which can then be used on a visualisation tool such as Data Studio, Tableau or PowerBI.

It also adds a cleaning functionality to Jinka, as the script automatically detects the list of expired offers and removes them from the Jinka website by sending a POST call to the website.

The application is intended to be used locally. Credentials are necessary in order to autenticate, however they never leave the computer which sends the request. For convenience purposes, a "Save credentials" option exists which saves the credentials on a local json. The credentials are not hashed, so use of this feature is at your own risk.

For the moment, the app has only been tested with the renting offer of Jinka.


# Installation

Python 3 is required for this application. The list of necessary packages is included in the requirements file.

You can install all of the necessary packages with the following command:

``` pip install -r requirements.txt ``` 

# Use

You can launch the application from the src folder by running :

``` python main.py ```

which will open the GUI. 

If you prefer to use the application on a command line, the app supports the following arguments:

``` -e --email ``` -> a string containing the email of the Jinka account  
``` -p --password ``` -> a password containing  
``` -l --load ``` -> 1 to load existing credentials, 0 not to load them. If 0 is specified, the email and password arguments must be filled  
``` -s --save ```-> 1 to save the credentials specified in the email and password fields  
``` -x --expired ``` -> 1 to clean all of the expired offers, 0 not to do it. This operation can be long to run as the script checks all of the active offers for expiration.  

Entering any argument will bypass the GUI. By default, load, save and expired are equal to 0.  

Examples :  

- Loading existing credentials and removing expired offers :
``` python main.py -l 1 -x 1 ```

- Writing new credentials, saving them and avoiding a check for expired offers : 
 
``` python main.py -e 'john.doe@gmail.com' -p '1234' -s 1 -x 0 ```  
or  
``` python main.py -e 'john.doe@gmail.com' -p '1234' -s 1```  

# Disclaimer

This project is not affiliated in any way with the Jinka team. Even though I had no issues with my account so far, I am not responsible for any action taken by Jinka against a given account as the script can adopt a suspicious behavior, especially during the cleaning of expired offers.