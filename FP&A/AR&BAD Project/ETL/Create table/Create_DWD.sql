select * from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Calendar
where fiscal_year_week_id=202401
drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Calendar
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Calendar
(
fiscal_year_id varchar(255),
fiscal_quarter_id varchar(255),
fiscal_quarter_code varchar(255),
fiscal_year_month_id varchar(255),
fiscal_year_week_id varchar(255),
FYM_fiscal_year_week_id varchar(255),
FYQ_fiscal_year_week_id varchar(255),
FY_fiscal_year_week_id varchar(255),
timestamps timestamp
)


select max("Fiscal Week Number") from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_WeeklyAR
where "Fiscal Week Number"=202501 limit 100
drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_WeeklyAR
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_WeeklyAR
(
fiscal_yearmonth varchar(255),
"Sold to Country Name" varchar(255),
"Fiscal Week Number" varchar(255),
"Company Code" varchar(255),
"Business Unit" varchar(255),
"Profit Center" varchar(255),
"Sold To Worldwide Customer Account Number" varchar(255),
"Sold To Worldwide Customer Account Name" varchar(255),	
"Sold To Customer Number" varchar(255),	
"Sold To Customer Name" varchar(255),	
"Payer Payment Terms Label" varchar(255),
"Payer Payment Terms Name" varchar(255),
"Sold To Sales Group Name" varchar(255),
"Sold To Sales Territory Name" varchar(255),
"Invoice Date" varchar(255),
"Invoice Due Date" varchar(255),
"invoice_net_budget_rate_amount" decimal(25,10),
current_due_budget_rate_amount decimal(25,10),
past_due_1_30_budget_rate_amount decimal(25,10),
past_due_31_60_budget_rate_amount decimal(25,10),
past_due_61_90_budget_rate_amount decimal(25,10),
past_due_91_120_budget_rate_amount decimal(25,10),
past_due_121_210_budget_rate_amount decimal(25,10),
past_due_211_300_budget_rate_amount decimal(25,10),
past_due_301_390_budget_rate_amount decimal(25,10),
past_due_391_480_budget_rate_amount decimal(25,10),
past_due_over_480_budget_rate_amount decimal(25,10),
"0-60 Bad Debt Reserve" decimal(25,10),
"61-90 Bad Debt Reserve" decimal(25,10),
"91-120 Bad Debt Reserve" decimal(25,10),
"121-200 Bad Debt Reserve" decimal(25,10),
"211-300 Bad Debt Reserve" decimal(25,10),
"301+ Bad Debt Reserve" decimal(25,10),
past_due_61_90_ByCustomer decimal(25,10),--用于计算baddebt 事前准备
past_due_91_120_ByCustomer decimal(25,10),
past_due_121_210_ByCustomer decimal(25,10),
past_due_211_300_ByCustomer decimal(25,10),
past_due_301_390_ByCustomer decimal(25,10),
past_due_391_480_ByCustomer decimal(25,10),
past_due_over_480_ByCustomer decimal(25,10),
BadDebt_31_60_budget_rate_amount decimal(25,10),
BadDebt_61_90_budget_rate_amount decimal(25,10),
BadDebt_91_120_budget_rate_amount decimal(25,10),
BadDebt_121_210_budget_rate_amount decimal(25,10),
BadDebt_211_300_budget_rate_amount decimal(25,10),
BadDebt_301_390_budget_rate_amount decimal(25,10),
BadDebt_391_480_budget_rate_amount decimal(25,10),
BadDebt_over_480_budget_rate_amount decimal(25,10),
timestamps timestamp
)

select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail limit 100
drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail
(
fiscal_year_month_id varchar(255),
fiscal_year_week_id varchar(255),
due_fiscal_year_month_id varchar(255),
due_fiscal_year_week_id varchar(255),
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
"Draft Amount" decimal(25,10),
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
"Acknowledgement Date" varchar(255),	--key
"SAP document" varchar(255),
"Received comment" varchar(255),
timestamps timestamp
)

select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_SalesBilling limit 100
drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_SalesBilling
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_SalesBilling
(
fiscal_year_week_id varchar(255),
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
"Order Amount - Budget Rate (C$)" decimal(25,10), 	
"Order Authoritative Margin - Budget Rate (C$)" decimal(25,10),	 
"Sales" decimal(25,10),  	 
"Margin" decimal(25,10), 	
"Product Line" varchar(255),	
"Sourcing" varchar(255),	
"GPL" varchar(255),	
"WWC" varchar(255),
timestamps timestamp
)

select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADAdjustment
where fiscal_year_week_id=202530
drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADAdjustment
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADAdjustment
(
fiscal_year_month_id varchar(255),
fiscal_year_week_id varchar(255),
"Company Code" varchar(255),
"Customer code" varchar(255),
Bad_Account_Occur_Amount decimal(28,10),
Bad_Account_Collection_Amount decimal(28,10),
Bad_Amount_detail decimal(28,10),
Adjustment decimal(28,10),
timestamps varchar(255)
)

select * from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_CustomerList
drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_CustomerList
create table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_CustomerList
(
"Start Year month" varchar(255),
"End Year month"  varchar(255),
Customer_Code varchar(255),
"Period" varchar(255), 	
"Period2" varchar(255),
startMonth int,
EndMonth int ,
"Period BAD Weight" varchar(255),
"Period BAD Weight2" float,
"Flag" varchar(255),		
"Longtime_BAD" varchar(255),
"Approved BAD Duration" varchar(255),
num int ,
timestamps varchar(255)
)

drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_Forecast_AR;
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_Forecast_AR
(
fiscal_yearmonth varchar(255),
fiscal_week  varchar(255),
"Company Code" varchar(255),
"Customer_Code"  varchar(255),	
AR_Due_amount decimal(25,10),
"Period BAD Weight" decimal(25,10),
"Flag" varchar(255),
"Longtime_BAD" varchar(255),
PastdueARPercentage_rate decimal(25,10),
Advanced_Payment decimal(25,10),
"BAD_Discount In advance" decimal(25,10),
timestamps varchar(255)
)

drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_WorldwideCustomer;
create table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_WorldwideCustomer
(
"Sold To Customer Number" varchar(255),	
"Sold To Worldwide Customer Account Number" varchar(255),
"Sold To Worldwide Customer Account Name" varchar(255),	
"Sold To Customer Name" varchar(255),
BU varchar(255),
Visual_Payment_term varchar(255),
timestamps varchar(255)
)

select * from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Customermasterdata
drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Customermasterdata
create table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Customermasterdata
(
"Customer" varchar(255),
"Company Code" varchar(255),--
Name varchar(255),	--
"Terms of Payment" varchar(255),--
"Payment term description" varchar(255),--
BU varchar(255),
"PGL group" varchar(255),
"WW Group" varchar(255),
"Payment term days" float,
BAD_Flag varchar(255),
timestamps varchar(255)
)

drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_BADCustomerLimit;
create table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_BADCustomerLimit
(
year varchar(255),
YM varchar(255),
"Start Year month"  varchar(255),
"End Year month" varchar(255),
Customer_Code varchar(255),
startMonth varchar(255),
EndMonth varchar(255),
"Period BAD Weight" float,
"Period BAD Weight2" float,
"Avg_Period BAD Weight" float,
Flag varchar(255),
"Longtime_BAD" varchar(255),
"Approved BAD Duration" varchar(255),
Months varchar(255),
timestamps varchar(255)
)

select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADEnd
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADEnd
(
fiscal_year_month_id varchar(255),
fiscal_year_week_id varchar(255),
fiscal_year_month_id_2 varchar(255),
fiscal_year_week_id_2 varchar(255),
due_fiscal_year_month_id varchar(255),
due_fiscal_year_week_id varchar(255),
"Entity code" varchar(255),
"Customer code" varchar(255),
"Draft Issuing Bank" varchar(255),	
"Branch Name Chinese" varchar(255),	
"Draft Amount" varchar(255),
timestamps varchar(255)
)

drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_OriginalActiveMaster
create table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_OriginalActiveMaster
(
"Customer" varchar(255),
"Company Code" varchar(255),--
"Name" varchar(255),	--
"Terms of Payment" varchar(255),--
"Payment term description" varchar(255),--
BU varchar(255),
Num varchar(255),
ranks int ,
timestamps varchar(255)
)

drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADmaturity
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADmaturity
(
due_fiscal_year_month varchar(255),
"Company Code" varchar(255),
Customer varchar(255),
Bad_Account_Occur_Amount float,
timestamps varchar(255)
)

drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADmaturity_APAC
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADmaturity_APAC
(
due_fiscal_year_month varchar(255),
fiscal_year_month_id  varchar(255),
"Company Code" varchar(255),
Customer varchar(255),
Bad_Account_Occur_Amount float,
timestamps varchar(255)
)