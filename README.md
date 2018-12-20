

Daily Balancing Script


For this project, I was tasked with pulling data from several systems and merging them by cost centers to ensure that the data matches across all platforms. 

The data sat in the following systems: SAP Hana, SAP Business Warehouse, AS400, and Margin Minder.

For SAP Hana and AS400 I connected to them via the library odbc. With SAP Business Warehouse I used the library RSAP which the most difficult aspect of this project. Margin Minder has macros that you can be scheduled and I used those macros and called them from R and could pull the data on command. I then merge all the data and wrote it to the AS400 server. I wrote it so that it would only add new data to the tables if the data was new, or different.

For this project my boss wanted to be able to refresh the data whenever she wanted. She has no experience with R and she didn't want me to put it on her machine. I worked with the networking and servers team to set up a windows VM. I mirrored the setup of my machine on the VM and set up the script to run every hour during business hours. That way my boss can refresh the pivot table and see the time that the data sources were last checked. 


