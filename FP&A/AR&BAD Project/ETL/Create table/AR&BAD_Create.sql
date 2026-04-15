select * from s3dataschema.ods_Finance_FPA_ARBAD_ARWeekly limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_ARWeekly
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_ARWeekly 
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Sold to Country Name" varchar(255),
"Fiscal Week Number" varchar(255),
"Company Code1" varchar(255),
"Business Unit" varchar(255),
"Profit Center" varchar(255),
"Sold To Worldwide Customer Account Number" varchar(255),
"Sold To Worldwide Customer Account Name" varchar(255),	
"Sold To Customer Number" varchar(255),	
"Sold To Customer Name" varchar(255),	
"Company Code2" varchar(255),	
"Payer Payment Terms Label" varchar(255),
"Payer Payment Terms Name" varchar(255),
"Sold To Sales Group Name" varchar(255),
"Sold To Sales Territory Name" varchar(255),
"Invoice Date" varchar(255),
"Invoice Due Date" varchar(255),
"Net Invoice Amount - Budget Rate" varchar(255),
"Open Receivable Amount - Budget Rate" varchar(255),
"Past Due Total Amount - Budget Rate" varchar(255),
"Past Due 1-30 Days Amount - Budget Rate" varchar(255),
"Past Due 31-60 Days Amount - Budget Rate" varchar(255),
"Past Due 61-90 Days Amount - Budget Rate" varchar(255),
"Past Due 91-120 Days Amount - Budget Rate" varchar(255),
"Past Due 121-210 Days Amount - Budget Rate" varchar(255),
"Past Due 211-300 Days Amount - Budget Rate" varchar(255),
"Past Due 301-390 Days Amount - Budget Rate" varchar(255),
"Past Due 391-480 Days Amount - Budget Rate" varchar(255),
"Past Due Over 480 Days Amount - Budget Rate" varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/TED/AR Weekly/';

select * from s3dataschema.ods_Finance_FPA_ARBAD_FBL3N limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_FBL3N
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_FBL3N 
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
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/FBL3N/';




select * from s3dataschema.ods_Finance_FPA_ARBAD_Billing 
where "Business Unit"='Business Unit';
drop table s3dataschema.ods_Finance_FPA_ARBAD_Billing
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_Billing 
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Business Unit" varchar(255),	
"Fiscal Year" varchar(255),	
"Fiscal Month Short Number" varchar(255),	
"Fiscal Month Label" varchar(255),	
"Fiscal Quarter Short Number" varchar(255),	
"Fiscal Week Short Number" varchar(255),	
"Company code" varchar(255),
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
"Sales" varchar(255),  	 
"Margin" varchar(255), 	
"Product Line" varchar(255),	
"Sourcing" varchar(255),	
"GPL" varchar(255),	
"WWC" varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Billing Date/';


select * from s3dataschema.ods_Finance_FPA_ARBAD_BADDetail limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_BADDetail
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_BADDetail 
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Entity code" varchar(255),
"BU" varchar(255),
"Request Date" varchar(255),
"Customer code" varchar(255),
"Remitter" varchar(255),
"TE Customer" varchar(255),
"TE Customer Chinese" varchar(255),	
"Draft Issuing Bank" varchar(255),	
"Branch Name Chinese" varchar(255),	
"Approved Bank" varchar(255),	
"Draft Amount" varchar(255),	
"Draft Issue date" varchar(255),	
"Draft Due Date" varchar(255),	
"Draft Number" varchar(255),	
"Draft type" varchar(255),	
"Requestor Name" varchar(255),	
"Number of Endorsement" varchar(255),	
"Number of Original Declaration" varchar(255),	
"Request Status" varchar(255),	
"Number of Declaration" varchar(255),	
"Approval request number" varchar(255),	
"Acknowledgement Date" varchar(255),	
"SAP document" varchar(255),
"Received comment" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/BAD Detail/';


select * from s3dataschema.ods_Finance_FPA_ARBAD_Banklist limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_Banklist
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_Banklist 
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Draft Issuing Bank" varchar(255),	
"Bank CN Name" varchar(255),	 
"GAD Limit (CNY)" varchar(255), 	
"Approved Bank Y/N" varchar(255),
Ranks varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/Approved BAD Bank list/';


select * from s3dataschema.ods_Finance_FPA_ARBAD_CustomerList limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_CustomerList
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_CustomerList 
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Start Year month" varchar(255),
"End Year month" varchar(255),
"Account" varchar(255),	
"Company Code" varchar(255),	
"Company Name" varchar(255),	
"Period" varchar(255), 	
"Period BAD Weight" varchar(255),	
"Flag" varchar(255),	
"Approved BAD Duration" varchar(255),	
"Longtime_BAD" varchar(255)


)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/BAD customer list/';

select * from s3dataschema.ods_Finance_FPA_ARBAD_Customermasterdata limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_Customermasterdata
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_Customermasterdata 
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Team" varchar(255),
"Company Code" varchar(255),
"Sales org." varchar(255),	
"Distribution Channel" varchar(255),	
"Customer" varchar(255),	
"Name" varchar(255),	
"Reconciliation acct" varchar(255),	
"Credit Control Area" varchar(255),	
"Credit account" varchar(255),
"Credit limit" varchar(255),
"Terms of Payment" varchar(255),
"Payment term description" varchar(255),
"Risk category" varchar(255),
"Credit rep.group" varchar(255),
"BU" varchar(255),
"Accounting clerk" varchar(255),
"Dunning clerk" varchar(255),
"Corporate Group" varchar(255),
"Last internal review" varchar(255),
"Next internal review" varchar(255),
"Created on" varchar(255),
"Country" varchar(255),
"Name 2" varchar(255),
"Name 3" varchar(255),
"Name 4" varchar(255),
"Name11" varchar(255),
"Name12" varchar(255),
"Name13" varchar(255),
"Name14" varchar(255),
"Street" varchar(255),
"Street_1" varchar(255),
"City" varchar(255),
"City_2" varchar(255),
"Industry business code" varchar(255),
"Industry Code" varchar(255),
"Sales Office" varchar(255),
"Sales Group" varchar(255),
"Sales Gr.Descipt" varchar(255),
"VAT Reg no" varchar(255),
"PO Box" varchar(255),
"Postal Code" varchar(255),
"Search term" varchar(255),
"One-time account" varchar(255),
"Region" varchar(255),
"Clerk's internet address" varchar(255),
"Tolerance group" varchar(255),
"Reason code conv" varchar(255),
"NRD over 1 year" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/Customer master data/';

select * from s3dataschema.ods_Finance_FPA_ARBAD_OpeningBalance limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_OpeningBalance
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_OpeningBalance 
(
"Filepath" varchar(255),
"Filename" varchar(255),
Fisical_week varchar(255),
Companycode varchar(255),
customer varchar(255),
AR_Openning varchar(255),
BAD_Openning varchar(255)



)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/F23_W52_OpeningBalance/';


select * from s3dataschema.ods_Finance_FPA_ARBAD_BaddebtRate limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_BaddebtRate
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_BaddebtRate
(
"Filepath" varchar(255),
"Filename" varchar(255),
"StartFYM" varchar(255),
"ExpiredFYM" varchar(255),
"AR PD Days" varchar(255),
"Bad Debt Reserve" float
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/BAD Debt/';



select * from s3dataschema.ods_Finance_FPA_ARBAD_PaymentTerm limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_PaymentTerm
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_PaymentTerm
(
"Filepath" varchar(255),
"Filename" varchar(255),
"Terms of Payment" varchar(255),
"Payment term description" varchar(255),
"Payment term days" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/Payment Term Days/';

select * from s3dataschema.ods_Finance_FPA_ARBAD_Taxrate limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_Taxrate
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_Taxrate
(
"Filename" varchar(255),
"sheetname" varchar(255),
"start FYM" varchar(255),
"Expired FYM" varchar(255),
"TAX Rate" varchar(255)
		

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/Tax rate/';


select 
SoldTo,
Salesorg,
Team,
"New Sales",
Customer,
"Customer Name",
"Group-Dashboard",
"Group-CAM"
from s3dataschema.ods_Finance_FPA_ARBAD_CustomerGroup
where Filename<>'filename'
where Filename='HM customer  Group list.xlsx' limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_CustomerGroup
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_CustomerGroup
(
"Filename" varchar(255),
"sheetname" varchar(255),
SoldTo	varchar(255),
Salesorg	varchar(255),
Team	varchar(255),
"New Sales"	varchar(255),
Customer	varchar(255),
"Customer Name" 	varchar(255),
"Group-Dashboard"	varchar(255),
"Group-CAM" varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/Customer group category/Customer Group/';

select * from s3dataschema.ods_Finance_FPA_ARBAD_PGLCustomerGroup 
where Filename<>'filename'
limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_PGLCustomerGroup
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_PGLCustomerGroup
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Sold-to" varchar(255),
"PGL group" varchar(255),
"WW Group" varchar(255),
"Sold-to Name" varchar(255),
Remark varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/Customer group category/Customer group mapping/WWGroup/';

select * from s3dataschema.ods_Finance_FPA_ARBAD_WWCustomerGroup
drop table s3dataschema.ods_Finance_FPA_ARBAD_WWCustomerGroup
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_WWCustomerGroup
(
"Filename" varchar(255),
"sheetname" varchar(255),
"WW Group" varchar(255),
"Payment_term" varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/Customer group category/Customer group mapping/WW Mapping/';


select 
FiscalWeek,
Advanced_Payment,
"BAD_Discount In advance"
from s3dataschema.ods_Finance_FPA_ARBAD_AdvancedPayment
where filename<>'filename' limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_AdvancedPayment
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_AdvancedPayment
(
"Filename" varchar(255),
"sheetname" varchar(255),
"FiscalWeek" varchar(255),
customer varchar(255),
"Advanced_Payment" varchar(255),
"BAD_Discount In advance" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Manual Input/Advanced_Payment/';

select * from s3dataschema.ods_Finance_FPA_ARBAD_PastdueARPercentage
where filename<>'filename' limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_PastdueARPercentage
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_PastdueARPercentage
(
"Filename" varchar(255),
"sheetname" varchar(255),
FiscalYM	 varchar(255),
rate varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Manual Input/Pastdue_AR Percentage/';


select * from s3dataschema.ods_Finance_FPA_ARBAD_ProfitCenter
where filename<>'filename' limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_ProfitCenter
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_ProfitCenter
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Profit center"	 varchar(255),
Industry varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Mapping/Profit center/';

select * from s3dataschema.ods_Finance_FPA_ARBAD_ARTarget
where filename<>'filename' limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_ARTarget
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_ARTarget
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Fiscal YM"	 varchar(255),
Amount varchar(255),
AR_Balance varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Manual Input/Target by YM/';

select * from s3dataschema.ods_Finance_FPA_ARBAD_DSOTarget
where filename<>'filename' limit 100;
drop table s3dataschema.ods_Finance_FPA_ARBAD_DSOTarget
CREATE external TABLE s3dataschema.ods_Finance_FPA_ARBAD_DSOTarget
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Fiscal YM"	 varchar(255),
Team varchar(255),
Target varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/AR_BAD_Report/Manual Input/Target by Year/';



select * from supply_chain.exploded_bill_of_material 
where root_part_number='1-936119-1'

parent_part_number='2410158-2'
root_organization_id in ('2554','1908','3027','1641') and root_part_number='1-936119-1'

limit 100
select * from supply_chain.exploded_bill_of_material_all
where parent_part_number='1-936119-1'


SELECT 
  *
FROM "ss_rs_ts_aut_ap_digital_db".finance."accounts_receivable_open_items_weekly"
where concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2)) >=202535 and company_code in 
('1473','0451','1321','1641','1908','3004','3027','2554') and business_unit_id in ('AUT','HES')



select 
distinct calendar_date
 from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date" 
where fiscal_year_week_id=202501


select 
distinct
fiscal_year_month_id,
fiscal_year_week_id,
to_char(calendar_date,'YYYYMMDD') as calendar_date
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_id>=2025 and fiscal_year_month_id=202508
order by calendar_date


select 
distinct
    fifiscal_year_begin_date ,
	fiscal_year_end_date,
    exchange_rate_USD
from temp_exchangerate
where fiscal_year_end_date=flag


select * FROM "ss_rs_ts_aut_ap_digital_db".finance."accounts_receivable_open_items_weekly"
where concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2)) >=202538 and company_code in 
('1473','0451','1321','1641','1908','3004','3027','2554') and business_unit_id in ('AUT','HES')


SELECT r.query, list_agg(s.text, '') WITHIN GROUP (ORDER BY s.sequence) AS full_statement
FROM stv_recents r
JOIN svl_statementtext s ON r.query = s.query
WHERE r.status IN ('Running', 'Compiled')
GROUP BY r.query
ORDER BY r.starttime DESC;


select *  from stv_recents 

where status in ('Running') limit 100

SELECT * FROM stv_inflight limit 100

select * from stv_recents limit 100



select pg_terminate_backend(1073901073);

SELECT 
 *
FROM svv_diskusage;


select
fiscal_year_week_id,
"Business Unit",	
"Fiscal Year",	
"Fiscal Month Short Number",	
"Fiscal Month Label",
"Fiscal Quarter Short Number",	
"Fiscal Week Short Number",	
"Company code",
"Sales Territory Level 5 Name",	
"Worldwide Customer Account Number",	
"Worldwide Customer Name",	
"Sold To Customer Number",	
"Sold To Customer Name",	
"Product Structure Level 1 Short Name (CBC1)",	
"Product Structure Level 2 (CBC2)",	
"Product Line (GPL)",	
"Procurement Source Company",	--Companycode
"Manufacturing Company",	 
"Order Amount - Budget Rate (C$)", 	
"Order Authoritative Margin - Budget Rate (C$)",	 
"Sales",  	 
"Margin", 	
"Product Line",	
"Sourcing",	
"GPL",	
"WWC"
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_SalesBilling
where "Fiscal Year"=2023


select 
filepath,
concat("Fiscal Year","Fiscal Week Short Number") as fiscal_year_week_id,
"Business Unit",	
"Fiscal Year",	
"Fiscal Month Short Number",	
"Fiscal Month Label",	
"Fiscal Quarter Short Number",	
"Fiscal Week Short Number",	
"Company code",
"Sales Territory Level 5 Name",	
"Worldwide Customer Account Number",	
"Worldwide Customer Name",	
"Sold To Customer Number",	
"Sold To Customer Name",	
"Product Structure Level 1 Short Name (CBC1)",	
"Product Structure Level 2 (CBC2)",	
"Product Line (GPL)",	
"Procurement Source Company",	
"Manufacturing Company",	 
cast("Order Amount - Budget Rate (C$)" as decimal(25,10)) as "Order Amount - Budget Rate", 	
cast("Order Authoritative Margin - Budget Rate (C$)" as decimal(25,10)) as "Order Authoritative Margin - Budget Rate",	 
cast("Sales" as decimal(25,10)) as Sales,  	 
cast("Margin" as decimal(25,10)) as Margin, 	
"Product Line",	
"Sourcing",	
"GPL",	
"WWC",
current_timestamp as timestamps
from s3dataschema.ods_Finance_FPA_ARBAD_Billing 
where "Fiscal Year"=2023;


select * from s3dataschema.ods_Finance_FPA_ARBAD_DSOTarget
