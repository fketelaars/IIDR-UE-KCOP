# IBM InfoSphere Data Replication User Exit - Kafka Custom Operator

## Installation
The GitHub repository contains the source but also the user exit in its compiled form, enclosed in a jar file. If you wish, you can build the jar file yourself using Ant, or to manually compile the user exit. If you wish to do so, please refer to the [Compilation](#compilation) section.

Download and unzip the master zip file from GitHub through the following link: [Download Zip](https://github.com/fketelaars/IIDR-UE-KCOP/archive/master.zip). The unzipped directory can be placed anywhere on the server that runs the CDC target engine; we recommend to unzip it under the CDC for Kafka engine's home directory (hereafter referred to as `<cdc_home>`)

## Configuration
In most scenarios you will need to perform a couple of configuration tasks:
- Update the configuration properties in the KcopLiveAuditJson.properties file
- Add the user exit and its configuration file to the classpath of the CDC *target* engine

### Setting the configuration properties
Update the `KcopLiveAuditJson.properties` file with your favourite editor.

An example of the LiveAudit properties can be found below
![User Exit Properties](Documentation/images/KcopLiveAudit_properties.png)

### Update the CDC engine's classpath
Assuming you have unzipped the file under the `<cdc_home>` directory, and the directory is called `IIDR-UE-KCOP-master`, add the following entries to the end of the classpath specified in the `<cdc_home>/conf/system.cp`: <br/>
`:IIDR-UE-KCOP-master/lib/*:IIDR-UE-KCOP-master`

Example classpath for CDC engine:
 ![Update Classpath](Documentation/images/Update_Classpath.png)
 
The `lib/*` classpath entry is needed to let CDC for Kafka find the jar file; the main directory holds the properties file that is read from within the KcopLiveAudit user exit.

Once you have updated the classpath, restart the CDC instance(s) for the changes to take effect.

## Configuring the subscription
Now that the setup tasks have been done and the user exit is available to the CDC engine, you must create a subscription that targets the CDC for Kafka engine and map the tables.

**Note:** Even though the user exit removes the need for the schema registry to be installed and configured, you will still need to reference a (dummy) schema registry host name and port name in the subscription's Kafka properties.

Finally, configure the subscription-level user exit. The full name of the user exit is: `com.ibm.replication.cdc.userexit.kcop.KcopLiveAuditJson`. An optional parameter can be specified: the name of the configuration file that must be read for this subscription. If not unspecified, the user exit will read its properties from the `KcopLiveAuditJson.properties` file. Please note that the properties file must be found in the classpath as specified in the previous steps.

![Subscription User Exit](Documentation/images/Configure_UE.png)

## Replicating changes
Below you will find an example of three change records: first a customer record is inserted, then updated and finally deleted. Before images are not included for the updated record. Also, we printed the Kafka message keys (this is the first JSON record on every line). If you did not specify a key, the key will be null.

```
{"CUSTNO":"876355"}	{"AUD_CCID":"0","AUD_ENTTYP":"I","AUD_TIMSTAMP":"2018-01-05 03:55:47.373000000000","AUD_USER":"DB2INST1","CUSTNO":"876355","BRANCH":"35","NAME1":"SOMMERVILLE NATIONAL LEASING","NAME2":" ","ADDRESS1":"255 DALESFORD RD.","ADDRESS2":" ","CITY":"LANSING","STATE":"MI","STATUS":"A","CRLIMIT":"49979","BALANCE":"45000","REPNO":"251","AUD_APPLY_TIMESTAMP":"2018-01-05T03:55:49.083309318Z"}
{"CUSTNO":"876355"}	{"AUD_CCID":"555057","AUD_ENTTYP":"U","AUD_TIMSTAMP":"2018-01-05 04:03:30.000000000000","AUD_USER":"DB2INST1","CUSTNO":"876355","BRANCH":"35","NAME1":"SOMMERVILLE NATIONAL LEASING","NAME2":" ","ADDRESS1":"255 DALESFORD RD.","ADDRESS2":" ","CITY":"LANSING","STATE":"MI","STATUS":"A","CRLIMIT":"49980","BALANCE":"45000","REPNO":"251","AUD_APPLY_TIMESTAMP":"2018-01-05T04:03:33.221713918Z"}
{"CUSTNO":"876355"}	{"AUD_CCID":"555058","AUD_ENTTYP":"D","AUD_TIMSTAMP":"2018-01-05 04:03:36.000000000000","AUD_USER":"DB2INST1","CUSTNO":"876355","BRANCH":"35","NAME1":"SOMMERVILLE NATIONAL LEASING","NAME2":" ","ADDRESS1":"255 DALESFORD RD.","ADDRESS2":" ","CITY":"LANSING","STATE":"MI","STATUS":"A","CRLIMIT":"49980","BALANCE":"45000","REPNO":"251","AUD_APPLY_TIMESTAMP":"2018-01-05T04:03:38.936274991Z"}
```

Example if no key was specified:
```
null	{"AUD_TIMSTAMP":"2018-01-05 05:17:51.004000000000","AUD_USER":"NOT SET   ","AUD_ENTTYP":"I","AUD_CCID":"0","PRODUCTID":"13644","DESCRIPTN":"cmqcNDdu","LOCATION":"VRDYAHgjwk","STATUS":"f","UNITPRICE":"2680732.16","UNITCOST":"1549338.86","QTYONHAND":"84873","QTYALLOC":"35004","QTYMINORD":"22259","AUD_APPLY_TIMESTAMP":"2018-01-05T05:17:52.705711297Z"}
```

## Compilation
If you wish to compile the user exit yourself, the easiest method is to use Ant ([https://ant.apache.org/bindownload.cgi](https://ant.apache.org/bindownload.cgi)). 

Once you have this installed:
- Ensure that the ant executable is in the path
- Go to the directory where you unzipped the user exit master file
- Update the `ant.properties` and update the `CDC_ENGINE_HOME` property to match the location where you installed the CDC engine
- Check the target version to be used (this is the Java version of the compiled objects) and should match the version of the Java Runtime Engine that is included with CDC
- Run `ant`
- First the sources will be compiled into their respective .class files and finally the class files are packaged into a jar file that is contained in the `lib` directory
