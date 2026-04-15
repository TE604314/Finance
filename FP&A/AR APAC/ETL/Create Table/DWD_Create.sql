select distinct fiscal_year_month_id 
from  ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Calendar limit 100;
drop table  ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Calendar
CREATE  TABLE ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Calendar
(
fiscal_year_id varchar(255),
fiscal_quarter_id varchar(255),
fiscal_quarter_code varchar(255),
fiscal_year_month_id varchar(255),
fiscal_year_week_id varchar(255),
FYM_fiscal_year_week_id varchar(255),
FYQ_fiscal_year_week_id varchar(255),
FY_fiscal_year_week_id varchar(255),
timestamps varchar(255)

)


select *  from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_WeeklyAR limit 100
where "Fiscal Week Number"=202501 limit 100
drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_WeeklyAR
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_WeeklyAR
(
fiscal_yearmonth varchar(255),
sub_region varchar(255),
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
past_due_61_90_ByCustomer decimal(25,10),--用于计算baddebt 事前准备
past_due_91_120_ByCustomer decimal(25,10),
past_due_121_210_ByCustomer decimal(25,10),
past_due_211_300_ByCustomer decimal(25,10),
past_due_301_390_ByCustomer decimal(25,10),
past_due_391_480_ByCustomer decimal(25,10),
past_due_over_480_ByCustomer decimal(25,10),
current_exchange_rate_USD  decimal(25,10),
newest_exchange_rate_USD  decimal(25,10),
timestamps timestamp
)


drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_BADmaturity
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_BADmaturity
(
due_fiscal_year_month varchar(255),
"Company Code" varchar(255),
Customer varchar(255),
Bad_Account_Occur_Amount float,
timestamps varchar(255)
)


select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_SalesBilling limit 100
drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_SalesBilling
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_SalesBilling
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
current_exchange_rate_USD decimal(25,10),
newest_exchange_rate_USD decimal(25,10),
timestamps timestamp
)


create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_BAD
(
fiscal_year_month_id varchar(255),
fiscal_year_week_id varchar(255),
"Company Code" varchar(255),
"Customer code" varchar(255),
Bad_Account_Occur_Amount decimal(25,10),
Bad_Account_Collection_Amount decimal(25,10),
timestamps timestamp
)

drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Customermasterdata
create table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Customermasterdata
(
"Sold To Customer Number" varchar(255),
"Company Code" varchar(255),
"Sales Office" varchar(255),
Name varchar(255),
"Terms of Payment" varchar(255),
"Payment term description" varchar(255),
"Credit limit" varchar(255),
BAD_Flag varchar(255),
"visual_WW group" varchar(255),
"Sold-to Name" varchar(255),
"Subregion" varchar(255),
"visual_payment" varchar(255),
"Entitled DSO*VAT" varchar(255),
"Entitled BAD%" varchar(255),
timestamps timestamp
)

create table ss_rs_ts_aut_ap_digital_schema.dwd_Finance_FPA_Apac_ARBAD_BADOpening
(
"Company Code"  varchar(255),
"Sold To Customer Number"  varchar(255),
bad_openning decimal(25,10),
timestamps varchar(255)
)


select
"Sold To Customer Number" as "Customer",
"Company Code",
"Sales Office",
Name,
"Terms of Payment",
"Payment term description",
"Credit limit",
BAD_Flag,
"visual_WW group",
"Sold-to Name",
"Subregion",
"visual_payment",
"Entitled DSO*VAT"
from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Customermasterdata