call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Apac_ARBAD_ODS_to_DWD_BillingSales()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Apac_ARBAD_ODS_to_DWD_BillingSales()
LANGUAGE plpgsql
AS $$
begin

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


delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_SalesBilling;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Apac_ARBAD_SalesBilling
(
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
current_exchange_rate_USD,
newest_exchange_rate_USD,
timestamps
)

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
"Procurement Source Company",	
"Manufacturing Company",	 
"Order Amount - Budget Rate"*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as "Order Amount - Budget Rate", 	
"Order Authoritative Margin - Budget Rate"*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as "Order Authoritative Margin - Budget Rate",
Sales*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as Sales,  	 
Margin*c.current_exchange_rate_USD/d.newest_exchange_rate_USD as Margin, 
c.current_exchange_rate_USD,
d.newest_exchange_rate_USD,
timestamps
from
(
select 
concat("Fiscal Year",right(cast("Fiscal Week Short Number" as int)+100,2)) as fiscal_year_week_id,
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
current_timestamp as timestamps
from s3dataschema.ods_Finance_FPA_Apac_ARBAD_Billing 
where filepath<>'filename' and "Company code" not in
('1473','0451','1321','1641','1908','3004','3027','2554')
) a 

LEFT JOIN
(
select 
    left(fiscal_year_end_date,4) as fiscal_year,
    fifiscal_year_begin_date ,
	fiscal_year_end_date,
    exchange_rate_USD as current_exchange_rate_USD
from temp_exchangerate
) c
on a."Fiscal Year"=c.fiscal_year

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


END;
$$;