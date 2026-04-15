
drop table ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Apac_ARBAD_ARBADSales_Combination
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Apac_ARBAD_ARBADSales_Combination
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
Bad_Account_Occur_Amount decimal(25,10),
Bad_Account_Collection_Amount decimal(25,10),
Billing_Sales decimal(25,10),  	 
Billing_Margin decimal(25,10),
timestamps timestamp
)


CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_Balance
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


create table ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_BADBalance
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
fiscal_yearmonth,
"Fiscal Week Number", 
"Company Code", 
"Sold To Customer Number",
past_due_1_30_budget_rate_amount,
past_due_31_60_budget_rate_amount,
past_due_61_90_budget_rate_amount,
past_due_91_120_budget_rate_amount,
past_due_121_210_budget_rate_amount,
past_due_211_300_budget_rate_amount,
past_due_301_390_budget_rate_amount,
past_due_391_480_budget_rate_amount,
past_due_over_480_budget_rate_amount,
current_due_budget_rate_amount,
ending_invoice_net_budget_rate_amount,
opening_invoice_net_budget_rate_amount,
Bad_Account_Occur_Amount,
Bad_Account_Collection_Amount,
Billing_Sales_includeVAT,
Billing_Sales,
Billing_Margin_includeVAT,
Billing_Margin,
"TAX Rate",
Collection
from ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_Balance
where "Fiscal Week Number"=202504


select
"Company Code", 
"Sold To Customer Number",
sum(ending_invoice_net_budget_rate_amount)
--opening_invoice_net_budget_rate_amount

from ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_Balance
where "Fiscal Week Number"=202501 and "Company Code"=1473 and "Sold To Customer Number"='1404583'
group by "Company Code",
"Sold To Customer Number"


select
"Company Code", 
"Sold To Customer Number",
sum(opening_invoice_net_budget_rate_amount)
--opening_invoice_net_budget_rate_amount

from ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_Balance
where "Fiscal Week Number"=202502 and "Company Code"=1473 and "Sold To Customer Number"='1404583'
group by "Company Code",
"Sold To Customer Number"
--"Sold To Customer Number"

