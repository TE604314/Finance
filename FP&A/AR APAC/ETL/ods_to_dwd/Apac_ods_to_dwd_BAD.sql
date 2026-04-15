
call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Apac_ARBAD_ODS_to_DWD_BAD()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Apac_ARBAD_ODS_to_DWD_BAD()
LANGUAGE plpgsql
AS $$
declare
maxym varchar;
begin
--"Posting Key",Industry,sum("Amount in local currency")
-- "Posting Key",sum("Amount in local currency")
select 
max(replace("Posting Date",'/','')) into maxym
from s3dataschema.ods_Finance_FPA_Apac_ARBAD_FBL3N 
where Filepath<>'filename' and Customer is not null and "Company Code" not in
('1473','0451','1321','1641','1908','3004','3027','2554');


create temporary table temp_exchangerate as 
select 
    left(to_char(effective_dt,'YYYYMMDD'),8) as fifiscal_year_begin_date ,
	left(to_char(expiration_dt,'YYYYMMDD'),8) as fiscal_year_end_date,
    currency_cde,
    max(left(to_char(expiration_dt,'YYYYMMDD'),8)) over(partition by 1) as flag,
    "rate_from_us_mult_fctr" as exchange_rate_USD
FROM "ss_rs_ts_aut_ap_digital_db"."master_data"."dimension_currency_exchange_rates_all"
WHERE currency_exchange_rate_cde=1 and
maxym between left(to_char(effective_dt,'YYYYMMDD'),8) and left(to_char(expiration_dt,'YYYYMMDD'),8)
;


create temporary table temp_BAD_Account AS
select
b.fiscal_year_month_id,
b.fiscal_year_week_id,
"Company Code",
Customer,
--"Account",
"Document Number",	
--"Plant",
--"Assignment",
--"Document Date",
"Posting Date",
"Document Type",
"Posting Key",	
"Amount in local currency",	
d.exchange_rate_USD,
"Local Currency",	
--"Amount in doc. curr",	
"Document currency",	
a."Profit Center",	
"Text",	
c.Industry,
b1.fiscal_year_month_id as due_fiscal_year_month,
b1.fiscal_year_week_id as due_fiscal_year_week,
"Net due date",
"Clearing date"
--"Reversed with"
from
(
select 
"Company Code",
Customer,
--"Account",
"Document Number",	
--"Plant",
--"Assignment",
--"Document Date",
replace("Posting Date",'/','') as "Posting Date",
"Document Type",
"Posting Key",	
"Amount in local currency",	
"Local Currency",	
--"Amount in doc. curr",	
"Document currency",	
"Profit Center",	
"Text",
replace("Net due date",'/','') AS "Net due date",
replace("Clearing date",'/','') AS "Clearing date",
"Arrears after net due date" 
--"Reversed with"
from s3dataschema.ods_Finance_FPA_Apac_ARBAD_FBL3N 
where Filepath<>'filename' and Customer is not null and  "Company Code" not in
('1473','0451','1321','1641','1908','3004','3027','2554') --过滤China
) a

left join 
(
select 
distinct
fiscal_year_month_id,
fiscal_year_week_id,
to_char(calendar_date,'YYYYMMDD') as calendar_date
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_id>=2025
) b
on a."Posting Date"=b.calendar_date


left join 
(
select 
distinct
fiscal_year_month_id,
fiscal_year_week_id,
to_char(calendar_date,'YYYYMMDD') as calendar_date
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_id>=2024 
) b1
on a."Net due date"=b1.calendar_date

left join
(
select 
"Profit center",
Industry
from  s3dataschema.ods_Finance_FPA_ARBAD_ProfitCenter
where filename<>'filename'
) c
on a."Profit Center"=c."Profit center"

left join
(
select 
distinct
    fifiscal_year_begin_date ,
	fiscal_year_end_date,
    currency_cde,
    exchange_rate_USD
from temp_exchangerate
where fiscal_year_end_date=flag
) d
on a."Local Currency"=d.currency_cde
--on b.calendar_date between d.fifiscal_year_begin_date and d.fiscal_year_end_date
;
delete ss_rs_ts_aut_ap_digital_schema.dwd_Finance_FPA_Apac_ARBAD_BADOpening;
insert into ss_rs_ts_aut_ap_digital_schema.dwd_Finance_FPA_Apac_ARBAD_BADOpening
(
"Company Code",
"Sold To Customer Number",
bad_openning,
timestamps
)
select
"Company Code",
"Sold To Customer Number",
bad_openning*b.exchange_rate_USD as bad_openning,
current_timestamp as timestamps
from
(
select 
1 as id,
companycode as "Company Code",
customer as "Sold To Customer Number",
sum(amount_usd)/7.24218 as bad_openning  --因为这是用TBR FY25
from s3dataschema.ods_Finance_FPA_Apac_ARBAD_BADOpening
where Filename<>'sheetname' and   companycode not in
('1473','0451','1321','1641','1908','3004','3027','2554')
group by 
companycode,
customer
) a

left join
(
select 
distinct
    1 as id,
    exchange_rate_USD
from temp_exchangerate
where fiscal_year_end_date=flag and currency_cde='CNY'
) b
on a.id=b.id;



--maturity BAD
delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_BADmaturity;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_BADmaturity
(
due_fiscal_year_month,
"Company Code",
Customer,
Bad_Account_Occur_Amount,
timestamps
)
select 
due_fiscal_year_month,
--due_fiscal_year_week,
"Company Code",
Customer,
sum( case when "Posting Key"='09' then cast("Amount in local currency" as float)/exchange_rate_USD else 0 end) as Bad_Account_Occur_Amount,
current_timestamp as timestamps
from temp_BAD_Account 
where fiscal_year_month_id is not null and Industry is not null and ("Clearing date"='' or left("Clearing date",8)>=left("Net due date",8))
group by 
due_fiscal_year_month,
"Company Code",
Customer;

--GAP 
delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_BAD;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_BAD
(
fiscal_year_month_id,
fiscal_year_week_id,
"Company Code",
"Customer code",
Bad_Account_Occur_Amount,
Bad_Account_Collection_Amount,
timestamps
)
select 
a.fiscal_year_month_id as fiscal_year_month_id,
a.fiscal_year_week_id as fiscal_year_week_id,
a."Company Code" as "Company Code",
a.Customer as "Customer code",
a.Bad_Account_Occur_Amount,
a.Bad_Account_Collection_Amount*-1 as Bad_Account_Collection_Amount,
current_timestamp as timestamps
from
(
select 
fiscal_year_month_id,
fiscal_year_week_id,
"Company Code",
Customer,
sum( case when "Posting Key"='09' then cast("Amount in local currency" as float)/exchange_rate_USD else 0 end) as Bad_Account_Occur_Amount,
sum(case when  "Posting Key"='19' then cast("Amount in local currency" as float)/exchange_rate_USD else 0 end ) as Bad_Account_Collection_Amount
 from temp_BAD_Account 
where fiscal_year_month_id is not null and Industry is not null 
group by 
fiscal_year_month_id,
fiscal_year_week_id,
"Company Code",
Customer
) a ;

drop table temp_BAD_Account;


END;
$$;


