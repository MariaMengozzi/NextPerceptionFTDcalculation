# FITNESS-TO-DRIVE CALCULATION CLIENT

## DESCRIPTION
This client MQTT aims at subscribing topics about cognitive/visual distraction and emotions. After getting the value from these topics, we can calculate the user's fitness-to-drive level according to the following formula:
$$FTD = 1 - (DCi + DVi + Ei)$$
Once the FTD level value has been calculated, the client publishes the value on topic _NP_UNIBO_FTD._

## PACKAGE TO INSTALL
In order to run this client must have install the following packages:
* python3 [link for python3 download](https://www.python.org/downloads/)
* paho [link for paho download](https://pypi.org/project/paho-mqtt/)
* pandas [link for pandas download](https://pandas.pydata.org/docs/getting_started/install.html)

## USAGE
Before running the ftd_calculation_client.py update:
* config.json file with the mqtt **broker name** and **port number**
* the variable **FTD_MAX_PUBLISH** with the number of FTD that must be calculated before inserting the value into the database.

### Note:
In order to test the ftd calculation client the speed was getting by a mqtt message on topic _Motion platform/Effective position_ 