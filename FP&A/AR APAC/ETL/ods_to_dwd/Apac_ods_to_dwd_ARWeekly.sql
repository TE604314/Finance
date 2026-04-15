
call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Apac_ARBAD_ODS_to_DWD_ARWeekly()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Apac_ARBAD_ODS_to_DWD_ARWeekly()
LANGUAGE plpgsql
AS $$
declare
maxym varchar;
BEGIN

select
b.fiscal_year_month_id  into maxym
from 
(
select 
left(max(replace("Posting Date",'/','')),8) as "Posting Date" from s3dataschema.ods_Finance_FPA_Apac_ARBAD_FBL3N 
where Filepath<>'filename' and Customer is not null and "Company Code" not in
('1473','0451','1321','1641','1908','3004','3027','2554')
) a 

left join 
(
select 
left(replace(calendar_date,'-',''),8) as calendar_date,
max(fiscal_year_month_id) as fiscal_year_month_id
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
group  by calendar_date
) b
on a."Posting Date"=b.calendar_date;

create temporary table temp_exchangerate as 
select 
    left(to_char(effective_dt,'YYYYMMDD'),8) as fifiscal_year_begin_date ,
	left(to_char(expiration_dt,'YYYYMMDD'),8) as fiscal_year_end_date,
    currency_cde,
    max(left(to_char(expiration_dt,'YYYYMMDD'),8)) over(partition by 1) as flag,
    "rate_from_us_mult_fctr" as exchange_rate_USD
FROM "ss_rs_ts_aut_ap_digital_db"."master_data"."dimension_currency_exchange_rates_all"
WHERE currency_exchange_rate_cde=1 and currency_cde='CNY'
;

--Calendar

delete ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Calendar;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Calendar
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
where fiscal_year_month_id>=202501 and fiscal_year_month_id<=maxym and fiscal_year_week_id<=
(
select cast(max(concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2))) as int) as fiscal_year_week_id FROM "ss_rs_ts_aut_ap_digital_db".finance."accounts_receivable_open_items_weekly"
where  company_region_name_level_1='Asia Pacific & ANZ' and business_unit_id in ('AUT','HES','PFM') AND company_code not in
('1473','0451','1321','1641','1908','3004','3027','2554')
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
where fiscal_year_month_id>=202501 and fiscal_year_month_id<=maxym and fiscal_year_week_id<=
(
select cast(max(concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2))) as int) as fiscal_year_week_id FROM "ss_rs_ts_aut_ap_digital_db".finance."accounts_receivable_open_items_weekly"
where  company_region_name_level_1='Asia Pacific & ANZ' and business_unit_id in ('AUT','HES','PFM') AND company_code not in
('1473','0451','1321','1641','1908','3004','3027','2554') --过滤China
)
) b
on a.fiscal_year_month_id=b.fiscal_year_month_id;


delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_WeeklyAR;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_WeeklyAR
(
fiscal_yearmonth,
sub_region,
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
past_due_61_90_ByCustomer,--用于计算baddebt 事前准备
past_due_91_120_ByCustomer,
past_due_121_210_ByCustomer,
past_due_211_300_ByCustomer,
past_due_301_390_ByCustomer,
past_due_391_480_ByCustomer,
past_due_over_480_ByCustomer,
current_exchange_rate_USD,
newest_exchange_rate_USD,
timestamps
)
select
b.fiscal_yearmonth,
sub_region,
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
"invoice_net_budget_rate_amount"*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as invoice_net_budget_rate_amount,
current_due_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as current_due_budget_rate_amount,
past_due_1_30_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_1_30_budget_rate_amount,
past_due_31_60_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_31_60_budget_rate_amount,
past_due_61_90_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_61_90_budget_rate_amount,
past_due_91_120_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_91_120_budget_rate_amount,
past_due_121_210_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_121_210_budget_rate_amount,
past_due_211_300_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_211_300_budget_rate_amount,
past_due_301_390_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_301_390_budget_rate_amount,
past_due_391_480_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_391_480_budget_rate_amount,
past_due_over_480_budget_rate_amount*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_over_480_budget_rate_amount,
past_due_61_90_ByCustomer*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_61_90_ByCustomer,--用于计算baddebt 事前准备
past_due_91_120_ByCustomer*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_91_120_ByCustomer,
past_due_121_210_ByCustomer*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_121_210_ByCustomer,
past_due_211_300_ByCustomer*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_211_300_ByCustomer,
past_due_301_390_ByCustomer*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_301_390_ByCustomer,
past_due_391_480_ByCustomer*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_391_480_ByCustomer,
past_due_over_480_ByCustomer*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as past_due_over_480_ByCustomer,
c.current_exchange_rate_USD,
d.newest_exchange_rate_USD,
CURRENT_TIMESTAMP as timestamp

from
(
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
    company_region_name_level_3 as sub_region,
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
where concat(reporting_fiscal_year,right(reporting_fiscal_week+100,2)) >=202501  and company_region_name_level_1='Asia Pacific & ANZ' and
business_unit_id in ('AUT','HES','PFM') AND company_code not in
('1473','0451','1321','1641','1908','3004','3027','2554')
group by
   sub_region,
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

LEFT JOIN
(
select 
    left(fiscal_year_end_date,4) as fiscal_year,
    fifiscal_year_begin_date ,
	fiscal_year_end_date,
    exchange_rate_USD as current_exchange_rate_USD
from temp_exchangerate
) c
on left(a."Fiscal Week Number",4)=c.fiscal_year

LEFT JOIN
(
select 
    left(fiscal_year_end_date,4) as fiscal_year,
    fifiscal_year_begin_date ,
	fiscal_year_end_date,
    exchange_rate_USD as newest_exchange_rate_USD
from temp_exchangerate
where left(fiscal_year_end_date,4) in (select max(left(fiscal_year_end_date,4)) from temp_exchangerate)
) d
on 1=1
;

drop table temp_exchangerate;

--Customermasterdata
delete ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Customermasterdata;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Customermasterdata
(
"Sold To Customer Number",
"Company Code",--
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
"Entitled DSO*VAT",
"Entitled BAD%",
timestamps
)
select
a."Sold To Customer Number",
a."Company Code",--
a."Sales Office",
a.Name,
a."Terms of Payment",
a."Payment term description",
a."Credit limit",
a.BAD_Flag,
b."visual_WW group",
b."Sold-to Name",
coalesce(c."Subregion",d.sub_region) as Subregion,
c."visual_payment",
c."Entitled DSO*VAT",
c."Entitled BAD%",
current_timestamp as timestamps
from
(
select
"Customer" as "Sold To Customer Number",
"Company Code", 
max("Sales Office") as "Sales Office",
max(F2) as Name,
max("Terms of Payment") as "Terms of Payment",
max("Payment term description") as "Payment term description",
max("Credit limit") as "Credit limit",
max("Note/BAD customer (Y)") as  BAD_Flag
from
 s3dataschema.ods_Finance_FPA_Apac_ARBAD_CustomerMaster 
where "Company Code" not in
('1473','0451','1321','1641','1908','3004','3027','2554') 
group by "Company Code","Customer"
) a 

left join

(
select
"Sold-to",
max("WW group") AS "WW group",
max("visual_WW group") AS "visual_WW group",
max("Sold-to Name") AS "Sold-to Name"

from  s3dataschema.ods_Finance_FPA_Apac_ARBAD_WWGroup
where "WW group"<>'WW Group'
group by "Sold-to"
) b
on a."Sold To Customer Number"=b."Sold-to"

left join 
(
select
"visual_WW group",
max("Subregion") as "Subregion",
max("visual_payment") as "visual_payment",
max("Entitled DSO*VAT") as "Entitled DSO*VAT",
max("Entitled BAD%") as "Entitled BAD%"
from s3dataschema.ods_Finance_FPA_Apac_ARBAD_WWMapping
group by "visual_WW group"
) c 
on b."visual_WW group"=c."visual_WW group"

--因为上面subregion 缺失
left join 
(
select
"Company Code",
"Sold To Customer Number",
max(case when sub_region='Japan'THEN 'JASEAN'else sub_region end) as sub_region

from  ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_WeeklyAR
group by 
"Company Code",
"Sold To Customer Number"
) d 
on a."Company Code"=d."Company Code" and a."Sold To Customer Number"=d."Sold To Customer Number";


END;
$$;
select max(fiscal_year_month_id) from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Apac_ARBAD_Calendar