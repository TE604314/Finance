
call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Apac_ARBAD_DWD_to_DWS_Combination()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Apac_ARBAD_DWD_to_DWS_Combination()
LANGUAGE plpgsql
AS $$
BEGIN

delete ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Apac_ARBAD_ARBADSales_Combination;
insert into ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Apac_ARBAD_ARBADSales_Combination
(
fiscal_yearmonth, --key
"Sold to Country Name",
"Fiscal Week Number", --key
"Company Code", --key
--Key_v1, --key
--Key_v2, --key
"Business Unit",
"Profit Center",
"Sold To Worldwide Customer Account Number",
"Sold To Worldwide Customer Account Name",	
"Sold To Customer Number", --key
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
Bad_Account_Occur_Amount,
Bad_Account_Collection_Amount,
Billing_Sales,  	 
Billing_Margin,
timestamps
)
select 
COALESCE(a.fiscal_yearmonth, b.fiscal_year_month_id, c.fiscal_year_month_id) as fiscal_yearmonth, --key
"Sold to Country Name",
COALESCE(a."Fiscal Week Number", b.fiscal_year_week_id, c.fiscal_year_week_id) as "Fiscal Week Number", --key
COALESCE(a."Company Code", b."Entity code",c.company_code) as "Company Code", --key
--COALESCE(a.Key_v1, b.Key_v1) as Key_v1, --key
--COALESCE(a.Key_v2, c.Key_v2) as Key_v2, --key
"Business Unit",
"Profit Center",
"Sold To Worldwide Customer Account Number",
"Sold To Worldwide Customer Account Name",	
COALESCE(a."Sold To Customer Number", b."Customer code", c."Sold To Customer Number") as "Sold To Customer Number", --key
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
b.Bad_Account_Occur_Amount,
b.Bad_Account_Collection_Amount,
c.Billing_Sales,  	 
c.Billing_Margin,
current_timestamp as timestamps
from 
(
select 
fiscal_yearmonth,
"Sold To Customer Number",	
"Fiscal Week Number", --key
"Company Code", --key
"Invoice Date", --key
"Invoice Due Date", --key
"Sold To Sales Group Name", --key,
"Sold To Sales Territory Name", --key,
row_number()over(partition by "Fiscal Week Number","Company Code","Sold To Customer Number") as Key_v1, --key
--row_number()over(partition by "Fiscal Week Number","Sold To Customer Number") as Key_v2, --key
max("Business Unit") as "Business Unit",
max("Profit Center") as "Profit Center",
max("Sold To Worldwide Customer Account Number") as "Sold To Worldwide Customer Account Number",
max("Sold To Worldwide Customer Account Name") as "Sold To Worldwide Customer Account Name",	
max("Sold to Country Name") as "Sold to Country Name",
max("Sold To Customer Name") as "Sold To Customer Name",	
max("Payer Payment Terms Label") as "Payer Payment Terms Label",
max("Payer Payment Terms Name") as "Payer Payment Terms Name",

sum("invoice_net_budget_rate_amount") as invoice_net_budget_rate_amount,
sum(current_due_budget_rate_amount) as current_due_budget_rate_amount,
sum(past_due_1_30_budget_rate_amount) as past_due_1_30_budget_rate_amount,
sum(past_due_31_60_budget_rate_amount) as past_due_31_60_budget_rate_amount,
sum(past_due_61_90_budget_rate_amount) as past_due_61_90_budget_rate_amount,
sum(past_due_91_120_budget_rate_amount) as past_due_91_120_budget_rate_amount,
sum(past_due_121_210_budget_rate_amount) as past_due_121_210_budget_rate_amount,
sum(past_due_211_300_budget_rate_amount) as past_due_211_300_budget_rate_amount,
sum(past_due_301_390_budget_rate_amount) as past_due_301_390_budget_rate_amount,
sum(past_due_391_480_budget_rate_amount) as past_due_391_480_budget_rate_amount,
sum(past_due_over_480_budget_rate_amount) as past_due_over_480_budget_rate_amount
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_WeeklyAR 
group by 
"Sold To Customer Number",	
fiscal_yearmonth,
"Fiscal Week Number", --key
"Company Code",
"Invoice Date", --key
"Invoice Due Date",
"Sold To Sales Group Name", --key,
"Sold To Sales Territory Name" --key,
) a

full outer join

(
select 
1 as Key_v1, --key
fiscal_year_month_id, --key
fiscal_year_week_id, --key
"Customer code", --key
"Company Code" as "Entity code", --key
sum(Bad_Account_Occur_Amount) as Bad_Account_Occur_Amount,
sum(Bad_Account_Collection_Amount) as Bad_Account_Collection_Amount
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_BAD 
group by 
fiscal_year_month_id,
fiscal_year_week_id,
"Company Code",
"Customer code"
) b
on  a.Key_v1=b.Key_v1 and a."Fiscal Week Number"=b.fiscal_year_week_id and a."Company Code"=b."Entity code" and a."Sold To Customer Number"=b."Customer code"


full outer join
(
select
1 as Key_v1,
"Company code" as company_code,
concat("Fiscal Year",right(cast("Fiscal Month Short Number" as int)+100,2))as fiscal_year_month_id,
case when length(fiscal_year_week_id)=5 then concat(left(fiscal_year_week_id,4),'0'+right(fiscal_year_week_id,1)) else
fiscal_year_week_id end as fiscal_year_week_id,
case when left("Sold To Customer Number",2)='00' then substring("Sold To Customer Number",3,15) 
     when left("Sold To Customer Number",1)='0' then substring("Sold To Customer Number",2,15) 
else "Sold To Customer Number" end as "Sold To Customer Number",	
sum("Sales") as Billing_Sales,  	 
sum("Margin") as Billing_Margin
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_SalesBilling
group by 
"Fiscal Year",
"Company code",
"Fiscal Month Short Number",
fiscal_year_week_id,
case when left("Sold To Customer Number",2)='00' then substring("Sold To Customer Number",3,15) 
     when left("Sold To Customer Number",1)='0' then substring("Sold To Customer Number",2,15) 
else "Sold To Customer Number" end) c

on   a.Key_v1=c.Key_v1 and a."Company Code"=c.company_code and a."Fiscal Week Number"=c.fiscal_year_week_id and a."Sold To Customer Number"=c."Sold To Customer Number";


create temporary table temp_fyweek as
select
a.fiscal_year_week_id,
c.fiscal_yearmonth as next_fiscal_yearmonth,
b.fiscal_year_week_id as last_fiscal_year_week_id,
c.fiscal_year_week_id as next_fiscal_year_week_id,
a.num
from
(
select
DISTINCT
fiscal_year_week_id,
ROW_NUMBER()over(partition by 1 order by fiscal_year_week_id ASC) as num

from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Calendar
order by fiscal_year_week_id asc
) a 

left join

(
select
DISTINCT
fiscal_year_week_id,
ROW_NUMBER()over(partition by 1 order by fiscal_year_week_id ASC)+1 as num

from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Calendar

union 

select
'202452' as fiscal_year_week_id,
1 as num
) b

on a.num=b.num

left join

(
select
DISTINCT
fiscal_year_month_id as fiscal_yearmonth,
fiscal_year_week_id,
ROW_NUMBER()over(partition by 1 order by fiscal_year_week_id ASC)-1 as num

from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Calendar
) c

on a.num=c.num;



create temporary table temp_balance as
select 
fiscal_yearmonth,
"Fiscal Week Number",
b.last_fiscal_year_week_id,
b.next_fiscal_yearmonth,
b.next_fiscal_year_week_id,
"Company Code",
"Sold To Customer Number", 
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
Bad_Account_Occur_Amount,
Bad_Account_Collection_Amount,
Billing_Sales,  	 
Billing_Margin
from 
(
select
fiscal_yearmonth, --key
"Fiscal Week Number", --key
"Company Code", --key
"Sold To Customer Number", --key
--ROW_NUMBER()over(partition by fiscal_yearmonth,"Fiscal Week Number" order by "Company Code","Sold To Customer Number" ASC) as Key_v1,
sum(coalesce("invoice_net_budget_rate_amount",0)) as "invoice_net_budget_rate_amount",
sum(coalesce(current_due_budget_rate_amount,0)) as current_due_budget_rate_amount,
sum(coalesce(past_due_1_30_budget_rate_amount,0)) as past_due_1_30_budget_rate_amount,
sum(coalesce(past_due_31_60_budget_rate_amount,0)) as past_due_31_60_budget_rate_amount,
sum(coalesce(past_due_61_90_budget_rate_amount,0)) as past_due_61_90_budget_rate_amount,
sum(coalesce(past_due_91_120_budget_rate_amount,0)) as past_due_91_120_budget_rate_amount,
sum(coalesce(past_due_121_210_budget_rate_amount,0)) as past_due_121_210_budget_rate_amount,
sum(coalesce(past_due_211_300_budget_rate_amount,0)) as past_due_211_300_budget_rate_amount,
sum(coalesce(past_due_301_390_budget_rate_amount,0)) as past_due_301_390_budget_rate_amount,
sum(coalesce(past_due_391_480_budget_rate_amount,0)) as past_due_391_480_budget_rate_amount,
sum(coalesce(past_due_over_480_budget_rate_amount,0)) as past_due_over_480_budget_rate_amount,
sum(coalesce(Bad_Account_Occur_Amount,0)) as Bad_Account_Occur_Amount,
sum(coalesce(Bad_Account_Collection_Amount,0)) as Bad_Account_Collection_Amount,
sum(coalesce(Billing_Sales,0)) as Billing_Sales,  	 
sum(coalesce(Billing_Margin,0)) as Billing_Margin
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Apac_ARBAD_ARBADSales_Combination
group by
fiscal_yearmonth, --key
"Fiscal Week Number", --key
"Company Code", --key
"Sold To Customer Number"
) a
 
left join
(
select
fiscal_year_week_id,
last_fiscal_year_week_id,
next_fiscal_yearmonth,
next_fiscal_year_week_id
from temp_fyweek
) b

on a."Fiscal Week Number"=b.fiscal_year_week_id;




create temporary table temp_balance_step2 as 
select
coalesce(a.fiscal_yearmonth,c.next_fiscal_yearmonth) as fiscal_yearmonth,
coalesce(a."Fiscal Week Number",c.next_fiscal_year_week_id) as "Fiscal Week Number",
coalesce(a."Company Code",c."Company Code") as "Company Code", 
coalesce(a."Sold To Customer Number",c."Sold To Customer Number") as "Sold To Customer Number",
coalesce(past_due_1_30_budget_rate_amount,0) as past_due_1_30_budget_rate_amount,
coalesce(past_due_31_60_budget_rate_amount,0) as past_due_31_60_budget_rate_amount,
coalesce(past_due_61_90_budget_rate_amount,0) as past_due_61_90_budget_rate_amount,
coalesce(past_due_91_120_budget_rate_amount,0) as past_due_91_120_budget_rate_amount,
coalesce(past_due_121_210_budget_rate_amount,0) as past_due_121_210_budget_rate_amount,
coalesce(past_due_211_300_budget_rate_amount,0) as past_due_211_300_budget_rate_amount,
coalesce(past_due_301_390_budget_rate_amount,0) as past_due_301_390_budget_rate_amount,
coalesce(past_due_391_480_budget_rate_amount,0) as past_due_391_480_budget_rate_amount,
coalesce(past_due_over_480_budget_rate_amount,0) as past_due_over_480_budget_rate_amount,
coalesce(current_due_budget_rate_amount,0) as current_due_budget_rate_amount,
coalesce(a."invoice_net_budget_rate_amount",0) as ending_invoice_net_budget_rate_amount,
coalesce(c.opening_invoice_net_budget_rate_amount,0) as opening_invoice_net_budget_rate_amount,
coalesce(Bad_Account_Occur_Amount,0) as Bad_Account_Occur_Amount,
coalesce(Bad_Account_Collection_Amount,0) as Bad_Account_Collection_Amount,
coalesce(a.Billing_Sales,0) as Billing_Sales,
coalesce(a.Billing_Margin,0) as Billing_Margin,
coalesce(d."TAX Rate",0) as "TAX Rate"
from 
(
select
fiscal_yearmonth, --key
"Fiscal Week Number", --key
last_fiscal_year_week_id,
"Company Code", --key
"Sold To Customer Number", --key
--key_v1,
"invoice_net_budget_rate_amount",
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
Bad_Account_Occur_Amount,
Bad_Account_Collection_Amount,
Billing_Sales,
Billing_Margin
from temp_balance
) a
 

full outer join

(
select
--1 as key_v1,
"Fiscal Week Number", --key
max(next_fiscal_yearmonth) as next_fiscal_yearmonth,
max(next_fiscal_year_week_id) as next_fiscal_year_week_id,
"Company Code", --key
"Sold To Customer Number", --key
sum("invoice_net_budget_rate_amount") as opening_invoice_net_budget_rate_amount
from temp_balance
group by "Fiscal Week Number","Company Code","Sold To Customer Number"

) c 
on a.last_fiscal_year_week_id=c."Fiscal Week Number"  and a."Company Code"=c."Company Code" and a."Sold To Customer Number"=c."Sold To Customer Number"

left join

(

select
startYM_fisical as "start FYM",
endYM_fisical as "Expired FYM",
companycode,
cast(VAT as float)+1 as "TAX Rate"
from s3dataschema.ods_Finance_FPA_Apac_ARBAD_VATRate 
where Filename<>'sheetname'
) d 
on a.fiscal_yearmonth between d."start FYM" and d."Expired FYM" and a."Company Code"=d.companycode;


drop table temp_balance;



--倒算collection
delete ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_Balance;
insert into ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_Balance
(
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
Collection,
timestamps
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
Billing_Sales*"TAX Rate" as Billing_Sales_includeVAT,
Billing_Sales,
Billing_Margin*"TAX Rate" as Billing_Margin_includeVAT,
Billing_Margin,
"TAX Rate",
opening_invoice_net_budget_rate_amount+Billing_Sales*"TAX Rate"-Bad_Account_Collection_Amount-ending_invoice_net_budget_rate_amount as Collection,
current_timestamp as timestamps
from temp_balance_step2;

drop table temp_balance_step2;



--全集Customer
create temporary table temp_Customerall as 
select
distinct
b.fiscal_yearmonth,
b."Fiscal Week Number",
a."Company Code", 
a."Sold To Customer Number",
c.BAD_Flag
from 
(
select 
distinct
"Company Code", 
"Sold To Customer Number"
from ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_Balance


union 

select
distinct
companycode  as "Company Code", 
customer as "Sold To Customer Number" --key
from
s3dataschema.ods_Finance_FPA_Apac_ARBAD_BADOpening
where Filepath<>'filename' and amount_usd is not null and amount_usd<>'' 

) a 

left join 

(
select
distinct 
fiscal_yearmonth,
"Fiscal Week Number"
from ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_Balance
order by "Fiscal Week Number"
) b
on 1=1

left join

(
select
distinct
"Company Code", 
"Customer" AS "Sold To Customer Number",
'BAD' as BAD_Flag
from s3dataschema.ods_Finance_FPA_Apac_ARBAD_CustomerMaster
where "Note/BAD customer (Y)"='Y'
) c
on a."Sold To Customer Number"=c."Sold To Customer Number" and a."Company Code"=c."Company Code"
;



create temporary table temp_BADBalance as 
select 
a.fiscal_yearmonth as fiscal_yearmonth,
--b.last_fiscal_year_week_id,
a."Fiscal Week Number" as "Fiscal Week Number", 
a."Company Code" as "Company Code",
a."Sold To Customer Number" as "Sold To Customer Number",
a.BAD_Flag,
c.bad_openning,
b.Bad_Account_Occur_Amount,
b.Bad_Account_Collection_Amount,
b.Bad_Account_Movement_Amount,
sum(b.Bad_Account_Movement_Amount)
over(partition by a."Company Code", a."Sold To Customer Number" order by a."Fiscal Week Number" asc rows between unbounded  preceding and CURRENT ROW) as BAD_YTD_Movement_Amount
--cast(c.bad_openning as decimal(25,10))+sum(b.Bad_Account_Movement_Amount)
--over(partition by a."Company Code", a."Sold To Customer Number" order by a."Fiscal Week Number" asc rows between unbounded  preceding and CURRENT ROW) as BAD_Ending

from 

(
select
distinct
fiscal_yearmonth,
"Fiscal Week Number",
"Company Code", 
"Sold To Customer Number",
BAD_Flag
FROM temp_Customerall
) a 

left join 
(
select 
fiscal_yearmonth,
"Fiscal Week Number", 
"Company Code", 
"Sold To Customer Number",
sum(Bad_Account_Occur_Amount) as Bad_Account_Occur_Amount,
sum(Bad_Account_Collection_Amount) as Bad_Account_Collection_Amount,
sum(Bad_Account_Occur_Amount) - sum(Bad_Account_Collection_Amount) as Bad_Account_Movement_Amount

from ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_Balance
group by 
fiscal_yearmonth,
"Fiscal Week Number",
"Company Code", 
"Sold To Customer Number"

) b
on a.fiscal_yearmonth=b.fiscal_yearmonth and a."Fiscal Week Number"=b."Fiscal Week Number" and a."Company Code"=b."Company Code" and a."Sold To Customer Number"=b."Sold To Customer Number"

left join

(
select 
"Company Code",
"Sold To Customer Number",
bad_openning  --因为这是用TBR FY25
from ss_rs_ts_aut_ap_digital_schema.dwd_Finance_FPA_Apac_ARBAD_BADOpening
) c
on a."Company Code"=c."Company Code" and a."Sold To Customer Number"=c."Sold To Customer Number";




drop table temp_Customerall;

drop table temp_fyweek;


delete ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_BADBalance;
insert into ss_rs_ts_aut_ap_digital_schema.DM_Finance_FPA_Apac_ARBAD_BADBalance
(
fiscal_yearmonth,
--last_fiscal_year_week_id,
"Fiscal Week Number", 
"Company Code",
"Sold To Customer Number",
"202401_bad_openning",
Bad_Account_Occur_Amount,
Bad_Account_Collection_Amount,
BAD_YTD_Movement_Amount,
BAD_Opening,
Bad_Account_Movement_Amount,
BAD_Ending,
timestamps
)
select
fiscal_yearmonth,
--last_fiscal_year_week_id,
"Fiscal Week Number", 
"Company Code",
"Sold To Customer Number",
bad_openning as "202401_bad_openning",
Bad_Account_Occur_Amount,
Bad_Account_Collection_Amount,
BAD_YTD_Movement_Amount,
coalesce(bad_openning,0)+coalesce(BAD_YTD_Movement_Amount,0)-coalesce(Bad_Account_Movement_Amount,0) as BAD_Opening,
Bad_Account_Movement_Amount,
coalesce(bad_openning,0)+coalesce(BAD_YTD_Movement_Amount,0) as BAD_Ending,
current_timestamp as timestamps
from temp_BADBalance
where coalesce(BAD_Opening,0)<>0 or coalesce(BAD_Ending,0)<>0 or coalesce(BAD_YTD_Movement_Amount,0)<>0 or BAD_Flag='BAD';

drop table temp_BADBalance;

END;
$$;

select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_WeeklyAR
where "company code"=1473 limit 100
