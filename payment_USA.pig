###Pharm_payment_USA
loadfile = LOAD 'hdfs://ip-10-0-0-157.ec2.internal:8020/user/ec2-user/input/medicare/OPPR_ALL_DTL_GNRL_12192014.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

pharmcost = FOREACH loadfile GENERATE (chararray) $8 AS physician_first,(chararray) $10 AS physician_last, (chararray) $15 AS state, (float) $48 AS Total_Payment;

cleanfile = FOREACH pharmcost GENERATE (CHARARRAY)REPLACE(physician_first,'[\\p{Punct},\\p{Cntrl}]','') AS Physician_First,(CHARARRAY)REPLACE(physician_last,'[\\p{Punct},\\p{Cntrl}]','') AS Physician_Last,(CHARARRAY)REPLACE(state,'[\\p{Punct},\\p{Cntrl}]','') AS State,Total_Payment;

clean = FOREACH cleanfile GENERATE (chararray)UPPER(CONCAT(Physician_First,' ',Physician_Last)) as practitioner,(chararray)State,(float)Total_Payment;

pharmcostPA1 = GROUP clean BY (practitioner,State);

pharmcostPA2 = FOREACH pharmcostPA1 GENERATE flatten(group) AS (practitioner, state),AVG(clean.Total_Payment) AS pharm_payment;


STORE pharmcostPA2 INTO 'hdfs://ip-10-0-0-157.ec2.internal:8020/user/ec2-user/winnie/FINAL_Project/py_pharmUSA.csv' USING PigStorage(',');


hadoop fs -copyToLocal winnie/FINAL_Project/py_pharmUSA.csv  ~/students/winnie/final_project/py_PharmUSA.csv

cat students/winnie/final_project/py_PharmUSA.csv/part-*>students/winnie/final_project/PyPharmUsa.csv

scp -i /Users/wenyingliu/Documents/summer/bigdata/gwu.pem  ec2-user@52.7.4.215:~/students/winnie/final_project/PyPharmUsa.csv  /Users/wenyingliu/Desktop/py_PharmUSA.csv


###Medicare_payment_USA
loadata = LOAD 'input/medicare/Medicare_Provider_Util_Payment_PUF_CY2012.txt' USING PigStorage('\t');

procedurecost= FOREACH loadata GENERATE (chararray) $1 AS provider_last, (chararray) $2 AS provider_first,(chararray) $11 AS state, (int) $16 AS code, (float) $26 AS average_medicare_payment;


cleandata = FOREACH procedurecost GENERATE (CHARARRAY)REPLACE(provider_last,'[\\p{Punct},\\p{Cntrl}]','') AS Provider_Last,(CHARARRAY)REPLACE(provider_first,'[\\p{Punct},\\p{Cntrl}]','') AS Provider_First,(CHARARRAY)REPLACE(state,'[\\p{Punct},\\p{Cntrl}]','') AS State,code,average_medicare_payment;


costdata = FOREACH cleandata GENERATE (chararray)UPPER(CONCAT(Provider_First,' ',Provider_Last)) as practitioner,(INT)code,(chararray)State,(float)average_medicare_payment;

costPA = FILTER costdata BY (code == 93015);

A = Group costPA by (practitioner,State);

LAST = FOREACH A GENERATE flatten(group) AS (practitioner,state),AVG(costPA.average_medicare_payment) AS medicare_payment;

STORE LAST INTO 'hdfs://ip-10-0-0-157.ec2.internal:8020/user/ec2-user/winnie/FINAL_Project/py_MEDUSA.csv' USING PigStorage(',');

hadoop fs -copyToLocal winnie/FINAL_Project/py_MEDUSA.csv  ~/students/winnie/final_project/py_MedUSA.csv

hadoop fs -copyToLocal winnie/FINAL_Project/py_medicareUSA*.csv  ~/students/winnie/final_project/py_MedUSA.csv

cat students/winnie/final_project/py_MedUSA.csv/part-*>students/winnie/final_project/PyMedUsa.csv

scp -i /Users/wenyingliu/Documents/summer/bigdata/gwu.pem  ec2-user@52.7.4.215:~/students/winnie/final_project/PyMedUsa.csv  /Users/wenyingliu/Desktop/py_medUSA2.csv









