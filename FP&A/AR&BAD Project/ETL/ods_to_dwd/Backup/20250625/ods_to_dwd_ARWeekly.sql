call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_ARBAD_ODS_to_DWD_ARWeekly()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_ARBAD_ODS_to_DWD_ARWeekly()
LANGUAGE plpgsql
AS $$
BEGIN

delete ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_CustomerList;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_CustomerList
(
"Start Year month",
"End Year month",
Customer_Code,
"Period", 	
"Period2",
startMonth,
EndMonth,
"Period BAD Weight",
"Period BAD Weight2",
"Flag",
"Longtime_BAD",
"Approved BAD Duration",
num,
timestamps
)
select 
"Start Year month",
"End Year month",
Customer_Code,
"Period", 	
split_part("Period",';',id) as "Period2",
cast(split_part(split_part("Period",';',id),'-',1) as int) as startMonth,
cast(split_part(split_part("Period",';',id),'-',2) as int) as EndMonth,
"Period BAD Weight",
case when "Period BAD Weight" like '%;%' and Flag='Ratio' then cast(replace(split_part("Period BAD Weight",';',id),'%','') as float)/100  else 
cast(replace(split_part("Period BAD Weight",';',id),'%','') as float) end as "Period BAD Weight2",
"Flag",		
"Longtime_BAD",
"Approved BAD Duration",
id as num,
current_timestamp as timestamps
from
(
select
"Start Year month",
"End Year month",
"Account" as Customer_Code,
"Period", 	
"Period BAD Weight",	
"Flag",	
"Approved BAD Duration",	
"Longtime_BAD"
from s3dataschema.ods_Finance_FPA_ARBAD_CustomerList 
where Filepath<>'filename'
) a

left join 

(
Select 
1 as id
union all
Select 
2 as id
union all
Select 
3 as id
union all
Select 
4 as id
) b
on 1=1
where length(split_part("Period",';',id))>1;





create temporary table temp_Customerlist as 
select 
c.Year,
"Start Year month",
"End Year month",
Customer_Code,
startMonth,
EndMonth,
"Period BAD Weight",
Flag,
"Longtime_BAD",
"Approved BAD Duration",
b.Months
from
(
select 
"Start Year month",
"End Year month",
Customer_Code,
startMonth,
EndMonth,
max("Period BAD Weight2") as "Period BAD Weight",
max("Flag") as Flag,
max("Longtime_BAD") as "Longtime_BAD",
max("Approved BAD Duration") as "Approved BAD Duration"
from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_CustomerList
group by 
"Start Year month",
"End Year month",
Customer_Code,
startMonth,
EndMonth
) a 

left join
(
Select 
1 as months
union all
Select 
2 as months
union all
Select 
3 as months
union all
Select 
4 as months
union all
Select 
5 as months
union all
Select 
6 as months
union all
Select 
7 as months
union all
Select 
8 as months
union all
Select 
9 as months
union all
Select 
10 as months
union all
Select 
11 as months
union all
Select 
12 as months
)  b
on 1=1

left join
(
select 
distinct "Fiscal Year" as Year
from s3dataschema.ods_Finance_FPA_ARBAD_Billing 
where filepath<>'filename'
)  c
on 1=1
where b.months between a.startMonth and a.EndMonth ;

delete ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_BADCustomerLimit;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_BADCustomerLimit
(
Year,
YM,
"Start Year month",
"End Year month",
Customer_Code,
startMonth,
EndMonth,
"Period BAD Weight",
"Period BAD Weight2",
"Avg_Period BAD Weight",
Flag,
"Longtime_BAD",
"Approved BAD Duration",
Months,
timestamps
)
select
Year,
 YM,
"Start Year month",
"End Year month",
Customer_Code,
startMonth,
EndMonth,
"Period BAD Weight",
"Period BAD Weight2",
case when Flag='Value Monthly' then "Period BAD Weight2" else "Period BAD Weight2"/Months end  as "Avg_Period BAD Weight",
Flag,
"Longtime_BAD",
"Approved BAD Duration",
Months,
current_timestamp as timestamps
from
(
select 
Year,
Year+RIGHT((Months+100),2) as YM,
"Start Year month",
"End Year month",
Customer_Code,
startMonth,
EndMonth,
"Period BAD Weight",
SUM("Period BAD Weight")OVER(PARTITION BY Customer_Code,"Start Year month","End Year month","Year" ORDER BY Months
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as "Period BAD Weight2",
Flag,
"Longtime_BAD",
"Approved BAD Duration",
Months
 from temp_Customerlist
WHERE Year+RIGHT((Months+100),2) BETWEEN "Start Year month" AND "End Year month"
) a ;

drop table temp_Customerlist;



--Calendar
delete ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Calendar;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Calendar
(
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
fiscal_year_month_id,
fiscal_year_week_id,
FYM_fiscal_year_week_id,
FYQ_fiscal_year_week_id,
FY_fiscal_year_week_id,
timestamps
)

select 
distinct
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
a.fiscal_year_month_id,
fiscal_year_week_id,
FYM_fiscal_year_week_id,
FYQ_fiscal_year_week_id,
FY_fiscal_year_week_id,
timestamps
from 
(
select 
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
fiscal_year_month_id,
fiscal_year_week_id,
getdate() as timestamps
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_id>=2024 and fiscal_year_week_id<=
(
select cast(max(concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2))) as int) as fiscal_year_week_id FROM "ss_rs_ts_aut_ap_digital_db".finance."accounts_receivable_open_items_weekly"
where concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2))>=202529  and company_code in 
('1473','0451','1321','1641','1908','3004','3027','2554') and business_unit_id in ('AUT','HES')
)
) a




left join 

(
select 
fiscal_year_month_id,
max(fiscal_year_week_id)over(partition by fiscal_year_month_id) as FYM_fiscal_year_week_id,
max(fiscal_year_week_id)over(partition by fiscal_quarter_id) as FYQ_fiscal_year_week_id,
max(fiscal_year_week_id)over(partition by fiscal_year_id) as FY_fiscal_year_week_id
 from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date" 
where fiscal_year_id>=2024 and fiscal_year_week_id<=
(
select cast(max(concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2))) as int) as fiscal_year_week_id FROM "ss_rs_ts_aut_ap_digital_db".finance."accounts_receivable_open_items_weekly"
where concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2)) >=202529 and company_code in 
('1473','0451','1321','1641','1908','3004','3027','2554') and business_unit_id in ('AUT','HES')
)
) b
on a.fiscal_year_month_id=b.fiscal_year_month_id;


delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_WeeklyAR;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_WeeklyAR
(
fiscal_yearmonth,
"Sold to Country Name",
"Fiscal Week Number",
"Company Code",
"Business Unit",
"Profit Center",
"Sold To Worldwide Customer Account Number",
"Sold To Worldwide Customer Account Name",	
"Sold To Customer Number",	
"Sold To Customer Name",	
"Payer Payment Terms Label",
"Payer Payment Terms Name",
"Sold To Sales Group Name",
"Sold To Sales Territory Name",
"Invoice Date",
"Invoice Due Date",
"invoice_net_budget_rate_amount",
current_due_budget_rate_amount,
past_due_1_30_budget_rate_amount,
past_due_31_60_budget_rate_amount,
past_due_61_90_budget_rate_amount,
past_due_91_120_budget_rate_amount,
past_due_121_210_budget_rate_amount,
past_due_211_300_budget_rate_amount,
past_due_301_390_budget_rate_amount,
past_due_391_480_budget_rate_amount,
past_due_over_480_budget_rate_amount,
"0-60 Bad Debt Reserve",
"61-90 Bad Debt Reserve",
"91-120 Bad Debt Reserve",
"121-200 Bad Debt Reserve",
"211-300 Bad Debt Reserve",
"301+ Bad Debt Reserve",
past_due_61_90_ByCustomer,--用于计算baddebt 事前准备
past_due_91_120_ByCustomer,
past_due_121_210_ByCustomer,
past_due_211_300_ByCustomer,
past_due_301_390_ByCustomer,
past_due_391_480_ByCustomer,
past_due_over_480_ByCustomer,
BadDebt_31_60_budget_rate_amount,
BadDebt_61_90_budget_rate_amount,
BadDebt_91_120_budget_rate_amount,
BadDebt_121_210_budget_rate_amount,
BadDebt_211_300_budget_rate_amount,
BadDebt_301_390_budget_rate_amount,
BadDebt_391_480_budget_rate_amount,
BadDebt_over_480_budget_rate_amount,
timestamps
)
select
b.fiscal_yearmonth,
"Sold to Country Name",
"Fiscal Week Number",
"Company Code",
"Business Unit",
"Profit Center",
"Sold To Worldwide Customer Account Number",
"Sold To Worldwide Customer Account Name",	
"Sold To Customer Number",	
"Sold To Customer Name",	
"Payer Payment Terms Label",
"Payer Payment Terms Name",
"Sold To Sales Group Name",
"Sold To Sales Territory Name",
"Invoice Date",
"Invoice Due Date",
"invoice_net_budget_rate_amount",
current_due_budget_rate_amount,
past_due_1_30_budget_rate_amount,
past_due_31_60_budget_rate_amount,
past_due_61_90_budget_rate_amount,
past_due_91_120_budget_rate_amount,
past_due_121_210_budget_rate_amount,
past_due_211_300_budget_rate_amount,
past_due_301_390_budget_rate_amount,
past_due_391_480_budget_rate_amount,
past_due_over_480_budget_rate_amount,
c."0-60 Bad Debt Reserve",
c."61-90 Bad Debt Reserve",
c."91-120 Bad Debt Reserve",
c."121-200 Bad Debt Reserve",
c."211-300 Bad Debt Reserve",
c."301+ Bad Debt Reserve",
past_due_61_90_ByCustomer,--用于计算baddebt 事前准备
past_due_91_120_ByCustomer,
past_due_121_210_ByCustomer,
past_due_211_300_ByCustomer,
past_due_301_390_ByCustomer,
past_due_391_480_ByCustomer,
past_due_over_480_ByCustomer,
0 as BadDebt_31_60_budget_rate_amount,
case when COALESCE(past_due_61_90_ByCustomer,0)>0 then past_due_61_90_budget_rate_amount*c."61-90 Bad Debt Reserve" else 0 end as BadDebt_61_90_budget_rate_amount,
case when COALESCE(past_due_91_120_ByCustomer,0)>0 then past_due_91_120_budget_rate_amount*c."91-120 Bad Debt Reserve" else 0 end as BadDebt_91_120_budget_rate_amount,
case when COALESCE(past_due_121_210_ByCustomer,0)>0 then past_due_121_210_budget_rate_amount*c."121-200 Bad Debt Reserve" else 0 end as BadDebt_121_210_budget_rate_amount,
case when COALESCE(past_due_211_300_ByCustomer,0)>0 then past_due_211_300_budget_rate_amount*c."211-300 Bad Debt Reserve" else 0 end as BadDebt_211_300_budget_rate_amount,
case when COALESCE(past_due_301_390_ByCustomer,0)>0 then past_due_301_390_budget_rate_amount*c."301+ Bad Debt Reserve" else 0 end as BadDebt_301_390_budget_rate_amount,
case when COALESCE(past_due_391_480_ByCustomer,0)>0 then past_due_391_480_budget_rate_amount*c."301+ Bad Debt Reserve"  else 0 end as BadDebt_391_480_budget_rate_amount,
case when COALESCE(past_due_over_480_ByCustomer,0)>0 then past_due_over_480_budget_rate_amount*c."301+ Bad Debt Reserve" else 0 end as BadDebt_over_480_budget_rate_amount,

CURRENT_TIMESTAMP as timestamp

from
(
--weekly AR history
select 
"Sold to Country Name",
"Fiscal Week Number",
"Company Code1" as "Company Code",
"Business Unit",
"Profit Center",
"Sold To Worldwide Customer Account Number",
"Sold To Worldwide Customer Account Name",	
"Sold To Customer Number",	
"Sold To Customer Name",	
"Payer Payment Terms Label",
"Payer Payment Terms Name",
"Sold To Sales Group Name",
"Sold To Sales Territory Name",
replace("Invoice Date",'/','') as "Invoice Date",
replace("Invoice Due Date",'/','') as "Invoice Due Date",
cast("Net Invoice Amount - Budget Rate" as float) as "invoice_net_budget_rate_amount",
cast("Past Due Total Amount - Budget Rate"as float) as current_due_budget_rate_amount,
cast("Past Due 1-30 Days Amount - Budget Rate" as float) as past_due_1_30_budget_rate_amount,
cast("Past Due 31-60 Days Amount - Budget Rate" as float) as past_due_31_60_budget_rate_amount,
cast("Past Due 61-90 Days Amount - Budget Rate" as float) as past_due_61_90_budget_rate_amount,
cast("Past Due 91-120 Days Amount - Budget Rate" as float) as past_due_91_120_budget_rate_amount,
cast("Past Due 121-210 Days Amount - Budget Rate" as float) as past_due_121_210_budget_rate_amount,
cast("Past Due 211-300 Days Amount - Budget Rate" as float) as past_due_211_300_budget_rate_amount,
cast("Past Due 301-390 Days Amount - Budget Rate" as float) as past_due_301_390_budget_rate_amount,
cast("Past Due 391-480 Days Amount - Budget Rate" as float) as past_due_391_480_budget_rate_amount,
cast("Past Due Over 480 Days Amount - Budget Rate" as float) as past_due_over_480_budget_rate_amount,
sum("Past Due 61-90 Days Amount - Budget Rate") over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_61_90_ByCustomer,--用于计算baddebt 事前准备
sum("Past Due 91-120 Days Amount - Budget Rate") over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_91_120_ByCustomer,
sum("Past Due 121-210 Days Amount - Budget Rate") over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_121_210_ByCustomer,
sum("Past Due 211-300 Days Amount - Budget Rate") over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_211_300_ByCustomer,
sum("Past Due 301-390 Days Amount - Budget Rate") over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_301_390_ByCustomer,
sum("Past Due 391-480 Days Amount - Budget Rate") over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_391_480_ByCustomer,
sum("Past Due Over 480 Days Amount - Budget Rate") over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_over_480_ByCustomer
from s3dataschema.ods_Finance_FPA_ARBAD_ARWeekly 
where "Fiscal Week Number"<202529 and Filepath<>'filename'

union all

select
*,
sum(past_due_61_90_budget_rate_amount) over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_61_90_ByCustomer,--用于计算baddebt 事前准备
sum(past_due_91_120_budget_rate_amount) over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_91_120_ByCustomer,
sum(past_due_121_210_budget_rate_amount) over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_121_210_ByCustomer,
sum(past_due_211_300_budget_rate_amount) over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_211_300_ByCustomer,
sum(past_due_301_390_budget_rate_amount) over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_301_390_ByCustomer,
sum(past_due_391_480_budget_rate_amount) over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_391_480_ByCustomer,
sum(past_due_over_480_budget_rate_amount) over(partition by "Fiscal Week Number","Sold To Customer Number") as past_due_over_480_ByCustomer
from
(
SELECT 
    "sold_to_customer_country_name" as "Sold to Country Name",
    concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2)) as "Fiscal Week Number",
    "company_code" as "Company Code",
    "business_unit_id" as "Business Unit",
    "profit_center_id" as "Profit Center",
    "sold_to_customer_worldwide_account_number_level_2" as "Sold To Worldwide Customer Account Number",
    "sold_to_customer_worldwide_account_name_level_2" as "Sold To Worldwide Customer Account Name",
    "sold_to_customer_partner_account_number" as "Sold To Customer Number",
    "sold_to_customer_name" as "Sold To Customer Name",
    "invoice_terms_code" as "Payer Payment Terms Label",
    "invoice_terms_description" as "Payer Payment Terms Name",
    "sold_to_customer_sales_group_name" as "Sold To Sales Group Name",
    "sold_to_customer_sales_territory_associate_name_level_7" as "Sold To Sales Territory Name",
    to_char("invoice_date",'YYYYMMDD') as "Invoice Date",
    to_char("invoice_due_date" ,'YYYYMMDD') as "Invoice Due Date",
    SUM("invoice_net_budget_rate_amount") as invoice_net_budget_rate_amount,
    SUM("past_due_1_30_budget_rate_amount")+SUM("past_due_31_60_budget_rate_amount")+SUM("past_due_61_90_budget_rate_amount")+SUM("past_due_91_120_budget_rate_amount")+
    SUM("past_due_121_210_budget_rate_amount")+SUM("past_due_211_300_budget_rate_amount")+SUM("past_due_301_390_budget_rate_amount")+
    SUM("past_due_391_480_budget_rate_amount")+SUM("past_due_over_480_budget_rate_amount") as current_due_budget_rate_amount,
    SUM("past_due_1_30_budget_rate_amount") as past_due_1_30_budget_rate_amount,
    SUM("past_due_31_60_budget_rate_amount") as past_due_31_60_budget_rate_amount,
    SUM("past_due_61_90_budget_rate_amount") as past_due_61_90_budget_rate_amount,
    SUM("past_due_91_120_budget_rate_amount") as past_due_91_120_budget_rate_amount,
    SUM("past_due_121_210_budget_rate_amount") as past_due_121_210_budget_rate_amount,
    SUM("past_due_211_300_budget_rate_amount") as past_due_211_300_budget_rate_amount,
    SUM("past_due_301_390_budget_rate_amount") as past_due_301_390_budget_rate_amount,
    SUM("past_due_391_480_budget_rate_amount") as past_due_391_480_budget_rate_amount,
    SUM("past_due_over_480_budget_rate_amount") as past_due_over_480_budget_rate_amount
    
    
FROM "ss_rs_ts_aut_ap_digital_db".finance."accounts_receivable_open_items_weekly"
where concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2)) >=202529 and company_code in 
('1473','0451','1321','1641','1908','3004','3027','2554') and business_unit_id in ('AUT','HES')
group by 
   "sold_to_customer_country_name",
   reporting_fiscal_year,
   reporting_fiscal_week,
    "company_code",
    "business_unit_id",
    "profit_center_id",
    "sold_to_customer_worldwide_account_number_level_2",
    "sold_to_customer_worldwide_account_name_level_2",
    "sold_to_customer_partner_account_number",
    "sold_to_customer_name",
    "invoice_terms_code",
    "invoice_terms_description",
    "sold_to_customer_sales_group_name",
    "sold_to_customer_sales_territory_associate_name_level_7",
    to_char("invoice_date",'YYYYMMDD'),
    to_char("invoice_due_date" ,'YYYYMMDD')
) a1
) a

left join 

(
select
distinct 
fiscal_year_week_id,
fiscal_year_month_id as fiscal_yearmonth
from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Calendar
) b 
on a."Fiscal Week Number"=b.fiscal_year_week_id

left join 

(
select 
"StartFYM",
"ExpiredFYM",
sum(case when "AR PD Days"='0-60' then "Bad Debt Reserve" end) as "0-60 Bad Debt Reserve",
sum(case when "AR PD Days"='61-90' then "Bad Debt Reserve" end) as "61-90 Bad Debt Reserve",
sum(case when "AR PD Days"='91-120' then "Bad Debt Reserve" end) as "91-120 Bad Debt Reserve",
sum(case when "AR PD Days"='121-200' then "Bad Debt Reserve" end) as "121-200 Bad Debt Reserve",
sum(case when "AR PD Days"='211-300' then "Bad Debt Reserve" end) as "211-300 Bad Debt Reserve",
sum(case when "AR PD Days"='301+' then "Bad Debt Reserve" end) as "301+ Bad Debt Reserve"
from s3dataschema.ods_Finance_FPA_ARBAD_BaddebtRate
where Filepath<>'filename'
group by 
"StartFYM",
"ExpiredFYM"
) c
on b.fiscal_yearmonth between c.StartFYM and c.ExpiredFYM;


--forecast
--sum(invoice_net_budget_rate_amount)/1000000
--select sum(invoice_net_budget_rate_amount)/1000000 from temp_AR_basis
--where due_fiscal_year_month_id between 202505 and 202507 and fiscal_week=202529

create temporary table temp_AR_basis as 
select
a.fiscal_yearmonth,
a.fiscal_week,
a."Company Code", --key
a."Sold To Customer Number",	
b.fiscal_year_month_id as due_fiscal_year_month_id,
b.fiscal_year_week_id as due_fiscal_year_week_id,
a."Invoice Due Date",
a."invoice_net_budget_rate_amount"
from
(
select 
"Company Code", 
"Sold To Customer Number",	
"Invoice Due Date",
fiscal_yearmonth,
"Fiscal Week Number" as fiscal_week,
sum("invoice_net_budget_rate_amount") as "invoice_net_budget_rate_amount"
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_WeeklyAR
group by 
"Company Code", 
"Sold To Customer Number",
fiscal_yearmonth,
"Fiscal Week Number",	
"Invoice Due Date"
) a 

left join 
(
select 
distinct
fiscal_year_month_id,
fiscal_year_week_id,
to_char(calendar_date,'YYYYMMDD') as calendar_date
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_id>=2024
) b 
on a."Invoice Due Date"=b.calendar_date;


create temporary table temp_last2month as 
select
fiscal_yearmonth,
fiscal_week,
Last2_YM, 
"Company Code", 
"Sold To Customer Number",	
due_fiscal_year_month_id,
due_fiscal_year_week_id,
"Invoice Due Date",
"invoice_net_budget_rate_amount"
from
(
select 
fiscal_yearmonth,
fiscal_week,
LEFT(dateadd(month,-2,to_date(left(fiscal_yearmonth,4)||'-'||right(fiscal_yearmonth,2)||'-01','YYYY-MM-DD')),4)||
Substring(dateadd(month,-2,to_date(left(fiscal_yearmonth,4)||'-'||right(fiscal_yearmonth,2)||'-01','YYYY-MM-DD')),6,2) as Last2_YM, 
"Company Code", 
"Sold To Customer Number",	
due_fiscal_year_month_id,
due_fiscal_year_week_id,
"Invoice Due Date",
"invoice_net_budget_rate_amount"
from temp_AR_basis
) a
where due_fiscal_year_month_id between  Last2_YM and fiscal_yearmonth;

drop table temp_AR_basis;

delete  ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_Forecast_AR;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_Forecast_AR
(
fiscal_yearmonth,
fiscal_week,
"Company Code",
"Customer_Code",	
AR_Due_amount,
"Period BAD Weight",
"Flag",
"Longtime_BAD",
PastdueARPercentage_rate,
Advanced_Payment,
"BAD_Discount In advance",
timestamps
)
select
a.fiscal_yearmonth,
a.fiscal_week,
a."Company Code",
a.Customer_Code,	
a.AR_Due_amount,
b."Period BAD Weight",
b."Flag",
b."Longtime_BAD",
c.PastdueARPercentage_rate,
d.Advanced_Payment,
d."BAD_Discount In advance",
current_timestamp as timestamps
from
(
select
fiscal_yearmonth,
fiscal_week,
"Company Code",
"Sold To Customer Number" as Customer_Code,	
sum("invoice_net_budget_rate_amount") as AR_Due_amount --当月加前两月-due date-due amount 加总,反应月底将会收到的款
from temp_last2month
group by
fiscal_yearmonth,
fiscal_week,
"Company Code",
"Sold To Customer Number"
) a

left join

(
select
distinct
"Start Year month",
"End Year month",
Customer_Code,	
startMonth,
EndMonth,
"Period BAD Weight2"  as "Period BAD Weight",
"Flag",
"Longtime_BAD"
from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_CustomerList
) b 
on a.fiscal_yearmonth between b."Start Year month" and b."End Year month" and a.Customer_Code=b.Customer_Code and
cast(right(a.fiscal_yearmonth,2) as int) between b.startMonth and EndMonth

left join 

(
select 
FiscalYM,
max(rate) as  PastdueARPercentage_rate
from s3dataschema.ods_Finance_FPA_ARBAD_PastdueARPercentage
where filename<>'filename'
GROUP BY FiscalYM
) c
on a.fiscal_yearmonth=c.FiscalYM

left join 
(
select 
FiscalWeek,
customer,
sum(Advanced_Payment) as Advanced_Payment,
sum("BAD_Discount In advance") as "BAD_Discount In advance"
from s3dataschema.ods_Finance_FPA_ARBAD_AdvancedPayment
where filename<>'filename'
group by 
FiscalWeek,
customer
) d
on a.fiscal_week=d.FiscalWeek AND a.Customer_Code=d.customer;


drop table temp_last2month;

delete  ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_WorldwideCustomer;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_WorldwideCustomer
(
"Sold To Customer Number",	
"Sold To Worldwide Customer Account Number",
"Sold To Worldwide Customer Account Name",	
"Sold To Customer Name",
BU,
timestamps
)
select 
"Sold To Customer Number",	
"Sold To Worldwide Customer Account Number",
"Sold To Worldwide Customer Account Name",	
"Sold To Customer Name",
b.BU,
timestamps
from 
(
select 
"Sold To Customer Number",	
max("Sold To Worldwide Customer Account Number") as "Sold To Worldwide Customer Account Number",
max("Sold To Worldwide Customer Account Name") as "Sold To Worldwide Customer Account Name",	
max("Sold To Customer Name") as "Sold To Customer Name",
current_timestamp as timestamps
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_WeeklyAR
WHERE "Sold To Customer Number" IS not null
group by "Sold To Customer Number"
) a 

left join 

(
select 
"Customer",	
max(REPLACE("BU",'AUT-','')) as BU
from s3dataschema.ods_Finance_FPA_ARBAD_Customermasterdata
GROUP BY Customer
) b
on a."Sold To Customer Number"=b.Customer;

--Customermasterdata
delete ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Customermasterdata;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_Customermasterdata
(
"Customer",
"Company Code",--
Name,	--
"Terms of Payment",--
"Payment term description",--
BU,
"PGL group",
"WW Group",
"Payment term days",
BAD_Flag,
timestamps
)
select
a."Customer",
"Company Code",--
Name,	--
a."Terms of Payment",--
a."Payment term description",--
BU,
case when b."PGL group" is null or b."PGL group"='' then 'Others' else  b."PGL group"  end  as "PGL group",
case when b."WW Group" is null or b."WW Group"='' then 'Others' else  b."WW Group" end  as "WW Group",
cast(c."Payment term days" as float) as "Payment term days",
d.BAD_Flag,
current_timestamp as timestamps
from
(
select
"Customer",
"Company Code",--
max("Name") as Name,	--
max("Terms of Payment") as "Terms of Payment",--
max("Payment term description") as "Payment term description",--
max("BU") as BU--
from s3dataschema.ods_Finance_FPA_ARBAD_Customermasterdata 
where Customer<>'Customer'
group by Customer,"Company Code"
) a 

left join

(
select
"Sold-to" as Customer,
max(case when "PGL group" is null or "PGL group"='' then 'Others' else  "PGL group"  end ) as "PGL group",
MAX(case when "WW Group" is null or "WW Group"='' then 'Others' else  "WW Group" end ) as "WW Group"
from s3dataschema.ods_Finance_FPA_ARBAD_PGLCustomerGroup
where "PGL group"<>'PGL Group'
group by "Sold-to"
) b
on a.Customer=b.Customer

left join 
(
select
"Terms of Payment",
max("Payment term description") as "Payment term description",
max("Payment term days") as "Payment term days"
from s3dataschema.ods_Finance_FPA_ARBAD_PaymentTerm
group by "Terms of Payment" 
) c 
on a."Terms of Payment"=c."Terms of Payment"

Left join
(
select
distinct
"Account",
'BAD' as BAD_Flag
from s3dataschema.ods_Finance_FPA_ARBAD_CustomerList 
) d
on a."Customer"=d.Account
;



END;
$$;

