##pharm_payment
loadfile = LOAD 'hdfs://ip-10-0-0-157.ec2.internal:8020/user/ec2-user/input/medicare/OPPR_ALL_DTL_GNRL_12192014.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

pharmcost = FOREACH loadfile GENERATE (chararray) $8 AS physician_first,(chararray) $10 AS physician_last, (chararray) $15 AS state, (float) $48 AS Total_Payment;

cleanfile = FOREACH pharmcost GENERATE (CHARARRAY)REPLACE(physician_first,'[\\p{Punct},\\p{Cntrl}]','') AS Physician_First,(CHARARRAY)REPLACE(physician_last,'[\\p{Punct},\\p{Cntrl}]','') AS Physician_Last,(CHARARRAY)REPLACE(state,'[\\p{Punct},\\p{Cntrl}]','') AS State,Total_Payment;

clean = FOREACH cleanfile GENERATE (chararray)UPPER(CONCAT(Physician_First,' ',Physician_Last)) as practitioner,(chararray)State,(float)Total_Payment;

pharmcostPA = FILTER clean BY (State matches '.*PA.*');


pharmcostPA1 = GROUP pharmcostPA BY practitioner;

pharmcostPA2 = FOREACH pharmcostPA1 GENERATE group AS practitioner,AVG(pharmcostPA.Total_Payment) AS pharm_payment;


STORE pharmcostPA2 INTO 'hdfs://ip-10-0-0-157.ec2.internal:8020/user/ec2-user/winnie/FINAL_Project/pharm_payment.csv' USING PigStorage(',');


hadoop fs -copyToLocal winnie/FINAL_Project/pharm_payment.csv  ~/students/winnie/final_project/Pharm_Payment.csv

scp -i /Users/wenyingliu/Documents/summer/bigdata/gwu.pem  ec2-user@52.7.4.215:~/students/winnie/final_project/Pharm_Payment.csv/part-r-00000  /Users/wenyingliu/Desktop/pharm_payment.csv

####





