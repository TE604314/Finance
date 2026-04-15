select * from s3dataschema.ods_Finance_FPA_Apac_ARBAD_FBL3N limit 100;
drop table s3dataschema.ods_Finance_FPA_Apac_ARBAD_FBL3N
CREATE external TABLE s3dataschema.ods_Finance_FPA_Apac_ARBAD_FBL3N 
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Company Code" varchar(255),
Account varchar(255),	
"Document Number" varchar(255),	
Plant varchar(255),
assignment varchar(255),
"Document date" varchar(255),
"Posting Date" varchar(255),
"Document Type" varchar(255),	
"Posting Key" varchar(255),	
"Amount in local currency" varchar(255),
"local currency" varchar(255),
"Amount in doc. curr." varchar(255),
"Document currency" varchar(255),	
"Profit Center" varchar(255),		
"Text" varchar(255),		
"reversed with" varchar(255),
"Customer" varchar(255),
"Net due date" varchar(255),
"Clearing date" varchar(255),
"Arrears after net due date" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Apac AR_BAD_Report/FBL3N/';


select * from s3dataschema.ods_Finance_FPA_Apac_ARBAD_Billing limit 10
drop table s3dataschema.ods_Finance_FPA_Apac_ARBAD_Billing
CREATE external TABLE s3dataschema.ods_Finance_FPA_Apac_ARBAD_Billing 
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Business Unit" varchar(255),
"Fiscal year" varchar(255),
"Fiscal Month Short Number" varchar(255),	
"Fiscal Month Label" varchar(255),	
"Fiscal Quarter Short Number" varchar(255),	
"Fiscal Week Short Number" varchar(255),	
"Company Code" varchar(255),	
"Sales Territory Level 5 Name" varchar(255),	
"Worldwide Customer Account Number" varchar(255),	
"Worldwide Customer Name" varchar(255),	
"Sold To Customer Number" varchar(255),	
"Sold To Customer Name" varchar(255),	
"Product Structure Level 1 Short Name (CBC1)" varchar(255),	
"Product Structure Level 2 (CBC2)" varchar(255),	
"Product Line (GPL)" varchar(255),	
"Procurement Source Company" varchar(255),	
"Manufacturing Company" varchar(255),	
"Order Amount - Budget Rate (C$)" varchar(255),	
"Order Authoritative Margin - Budget Rate (C$)" varchar(255),	
"sales" varchar(255),	
"margin" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Apac AR_BAD_Report/Billing Data/';

select * from s3dataschema.ods_Finance_FPA_Apac_ARBAD_CustomerMaster limit 10
drop table s3dataschema.ods_Finance_FPA_Apac_ARBAD_CustomerMaster
CREATE external TABLE s3dataschema.ods_Finance_FPA_Apac_ARBAD_CustomerMaster 
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Company Code" varchar(255), 
"Customer" varchar(255),
"Sales Office" varchar(255),
F1 varchar(255),
F2 varchar(255),
F3 varchar(255),
F4 varchar(255),
F5 varchar(255),
"Credit limit" varchar(255),
"Terms of Payment" varchar(255),
"Payment term description" varchar(255),
F6 varchar(255),
F7 varchar(255),
F8 varchar(255),
F9 varchar(255),
F10 varchar(255),
F11 varchar(255),
F12 varchar(255),
F13 varchar(255),
F14 varchar(255),
F15 varchar(255),
F16 varchar(255),
F17 varchar(255),
F18 varchar(255),
F19 varchar(255),
F20 varchar(255),
F21 varchar(255),
F22 varchar(255),
F23 varchar(255),
F24 varchar(255),
F25 varchar(255),
F26 varchar(255),
F27 varchar(255),
F28 varchar(255),
F29 varchar(255),
F30 varchar(255),
F31 varchar(255),
F32 varchar(255),
F33 varchar(255),
F34 varchar(255),
F35 varchar(255),
F36 varchar(255),
F37 varchar(255),
"Note/BAD customer (Y)" varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Apac AR_BAD_Report/Mapping/Customer master data/';


select * from s3dataschema.ods_Finance_FPA_Apac_ARBAD_ARTarget limit 10
drop table s3dataschema.ods_Finance_FPA_Apac_ARBAD_ARTarget
CREATE external TABLE s3dataschema.ods_Finance_FPA_Apac_ARBAD_ARTarget 
(
"Filepath" varchar(255),
"Filename" varchar(255),
FiscalYM varchar(255),
Region varchar(255),
AR_Balance varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Apac AR_BAD_Report/Manual Input/Target by YM/';


select * from s3dataschema.ods_Finance_FPA_Apac_ARBAD_DSOTarget limit 10
drop table s3dataschema.ods_Finance_FPA_Apac_ARBAD_DSOTarget
CREATE external TABLE s3dataschema.ods_Finance_FPA_Apac_ARBAD_DSOTarget 
(
"Filepath" varchar(255),
"Filename" varchar(255),
FiscalYM varchar(255),
Region varchar(255),
"DSO target (Days)" varchar(255)


)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Apac AR_BAD_Report/Manual Input/Target by Year/';


select * from s3dataschema.ods_Finance_FPA_Apac_ARBAD_VATRate limit 10
drop table s3dataschema.ods_Finance_FPA_Apac_ARBAD_VATRate
CREATE external TABLE s3dataschema.ods_Finance_FPA_Apac_ARBAD_VATRate 
(
"Filepath" varchar(255),
"Filename" varchar(255),
startYM_fisical varchar(255),
endYM_fisical varchar(255),
companycode varchar(255),
des varchar(255),
VAT varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Apac AR_BAD_Report/Mapping/VAT/';


select * from s3dataschema.ods_Finance_FPA_Apac_ARBAD_BADOpening limit 10
drop table s3dataschema.ods_Finance_FPA_Apac_ARBAD_BADOpening
CREATE external TABLE s3dataschema.ods_Finance_FPA_Apac_ARBAD_BADOpening
(
"Filepath" varchar(255),
"Filename" varchar(255),
companycode varchar(255),
customer varchar(255),
localcurreny varchar(255),
amount_localcurrency varchar(255),
amount_usd varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Apac AR_BAD_Report/Opening Balance/';



select * from s3dataschema.ods_Finance_FPA_Apac_ARBAD_WWGroup limit 10
drop table s3dataschema.ods_Finance_FPA_Apac_ARBAD_WWGroup
CREATE external TABLE s3dataschema.ods_Finance_FPA_Apac_ARBAD_WWGroup
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Sold-to" varchar(255),
"WW group" varchar(255),
"visual_WW group" varchar(255),
"Sold-to Name" varchar(255),
"Remark" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Apac AR_BAD_Report/Mapping/Customer group mapping/WW Group/';


select * from s3dataschema.ods_Finance_FPA_Apac_ARBAD_WWMapping limit 10
drop table s3dataschema.ods_Finance_FPA_Apac_ARBAD_WWMapping
CREATE external TABLE s3dataschema.ods_Finance_FPA_Apac_ARBAD_WWMapping
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Subregion" varchar(255),
"visual_WW group" varchar(255),
"visual_payment" varchar(255),
"Entitled DSO*VAT" varchar(255),
"Entitled BAD%" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Apac AR_BAD_Report/Mapping/Customer group mapping/WW Mapping/';

