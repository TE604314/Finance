select max(timestamps) from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_ARBAD_ARBADSales_Combination 
where fiscal_yearmonth=202401 and "Fiscal Week Number"=202401
drop table ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_ARBAD_ARBADSales_Combination
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_ARBAD_ARBADSales_Combination
(
fiscal_yearmonth varchar(255), --key
"Sold to Country Name" varchar(255),
"Fiscal Week Number" varchar(255), --key
"Company Code" varchar(255), --key
--Key_v1 varchar(255), --key
--Key_v2 varchar(255), --key
"Business Unit" varchar(255),
"Profit Center" varchar(255),
"Sold To Worldwide Customer Account Number" varchar(255),
"Sold To Worldwide Customer Account Name" varchar(255),	
"Sold To Customer Number" varchar(255), --key
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
BadDebt_31_60_budget_rate_amount decimal(25,10),
BadDebt_61_90_budget_rate_amount decimal(25,10),
BadDebt_91_120_budget_rate_amount decimal(25,10),
BadDebt_121_210_budget_rate_amount decimal(25,10),
BadDebt_211_300_budget_rate_amount decimal(25,10),
BadDebt_301_390_budget_rate_amount decimal(25,10),
BadDebt_391_480_budget_rate_amount decimal(25,10),
BadDebt_over_480_budget_rate_amount decimal(25,10),
Bad_Account_Occur_Amount decimal(25,10),
Bad_Account_Collection_Amount decimal(25,10),
Billing_Sales decimal(25,10),  	 
Billing_Margin decimal(25,10),
timestamps timestamp
)

drop table ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_ARBAD_Forecast_AR
select * from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_ARBAD_Forecast_AR limit 100
create table ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_ARBAD_Forecast_AR
(
fiscal_yearmonth varchar(255),
fiscal_week varchar(255),
"Company code"  varchar(255),
"Customer_Code" varchar(255),	
AR_Due_amount decimal(38,10),
"BAD receive amount(assumed)" decimal(38,10),
"AR due amount cash" decimal(38,10),
"AR collect amount(assumed)" decimal(38,10),
"BAD discount amount" decimal(38,10),
"Period BAD Weight" decimal(38,10),
Monthly_BADDiscount decimal(38,10),
WTD_BADDiscount_OneMonth decimal(38,10),
Outstanding_BAD decimal(38,10),
"Flag" varchar(255),
"Longtime_BAD" varchar(255),
PastdueARPercentage_rate decimal(38,10),
Advanced_Payment decimal(38),
"BAD_Discount In advance" decimal(38,10),
Monthly_Amount decimal(38,10),
YTD_Amount decimal(38,10),
timestamps varchar(255)
)

select sum(ending_invoice_net_budget_rate_amount) from ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_ARBAD_Balance
where "Fiscal Week Number"=202529;
drop table ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_ARBAD_Balance;
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_ARBAD_Balance
(
fiscal_yearmonth varchar(255),
"Fiscal Week Number" varchar(255), 
"Company Code" varchar(255), 
"Sold To Customer Number" varchar(255),
past_due_1_30_budget_rate_amount decimal(25,10),
past_due_31_60_budget_rate_amount decimal(25,10),
past_due_61_90_budget_rate_amount decimal(25,10),
past_due_91_120_budget_rate_amount decimal(25,10),
past_due_121_210_budget_rate_amount decimal(25,10),
past_due_211_300_budget_rate_amount decimal(25,10),
past_due_301_390_budget_rate_amount decimal(25,10),
past_due_391_480_budget_rate_amount decimal(25,10),
past_due_over_480_budget_rate_amount decimal(25,10),
current_due_budget_rate_amount decimal(25,10),
BadDebt_31_60_budget_rate_amount decimal(25,10),
BadDebt_61_90_budget_rate_amount decimal(25,10),
BadDebt_91_120_budget_rate_amount decimal(25,10),
BadDebt_121_210_budget_rate_amount decimal(25,10),
BadDebt_211_300_budget_rate_amount decimal(25,10),
BadDebt_301_390_budget_rate_amount decimal(25,10),
BadDebt_391_480_budget_rate_amount decimal(25,10),
BadDebt_over_480_budget_rate_amount decimal(25,10),
BadDebt_budget_rate_amount_total decimal(25,10),
ending_invoice_net_budget_rate_amount decimal(25,10),
opening_invoice_net_budget_rate_amount decimal(25,10),
Bad_Account_Occur_Amount decimal(25,10),
Bad_Account_Collection_Amount decimal(25,10),
Billing_Sales_includeVAT decimal(25,10),
Billing_Sales decimal(25,10),
Billing_Margin_includeVAT decimal(25,10),
Billing_Margin decimal(25,10),
"TAX Rate" decimal(25,10),
Collection decimal(25,10),
timestamps timestamp
)

drop table ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_ARBAD_BADBalance
create table ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_ARBAD_BADBalance
(
fiscal_yearmonth VARCHAR(255),
--last_fiscal_year_week_id VARCHAR(255),
"Fiscal Week Number" VARCHAR(255), 
"Company Code" VARCHAR(255),
"Sold To Customer Number" VARCHAR(255),
"202401_bad_openning" decimal(25,10),
Bad_Account_Occur_Amount decimal(25,10),
Bad_Account_Collection_Amount decimal(25,10),
BAD_YTD_Movement_Amount decimal(25,10),
BAD_Opening decimal(25,10),
Bad_Account_Movement_Amount decimal(25,10),
BAD_Ending decimal(25,10),
timestamps VARCHAR(255)
)


select 
distinct
fiscal_year_month_id,
fiscal_year_week_id,
to_char(calendar_date,'YYYYMMDD') as calendar_date
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_id>=2024 and fiscal_year_week_id=202535