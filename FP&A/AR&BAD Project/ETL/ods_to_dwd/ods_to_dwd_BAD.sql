
call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_ARBAD_ODS_to_DWD_BAD()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_ARBAD_ODS_to_DWD_BAD()
LANGUAGE plpgsql
AS $$
declare
maxym varchar;
begin
--"Posting Key",Industry,sum("Amount in local currency")
-- "Posting Key",sum("Amount in local currency")
select 
max(replace("Posting Date",'/','')) into maxym
from s3dataschema.ods_Finance_FPA_ARBAD_FBL3N 
where Filepath<>'filename' and Customer is not null;


create temporary table temp_exchangerate as 
select 
    left(to_char(effective_dt,'YYYYMMDD'),8) as fifiscal_year_begin_date ,
	left(to_char(expiration_dt,'YYYYMMDD'),8) as fiscal_year_end_date,
    max(left(to_char(expiration_dt,'YYYYMMDD'),8)) over(partition by 1) as flag,
    "rate_from_us_mult_fctr" as exchange_rate_USD
FROM "ss_rs_ts_aut_ap_digital_db"."master_data"."dimension_currency_exchange_rates_all"
WHERE currency_exchange_rate_cde=1 and currency_cde='CNY' and 
/*
(
select 
max(replace("Posting Date",'/','')) as "Posting Date"
from s3dataschema.ods_Finance_FPA_ARBAD_FBL3N 
where Filepath<>'filename' and Customer is not null
)

'20250701' between 

left(to_char(effective_dt,'YYYYMMDD'),8) and left(to_char(expiration_dt,'YYYYMMDD'),8) */
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
--"Local Currency",	
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
--"Local Currency",	
--"Amount in doc. curr",	
"Document currency",	
"Profit Center",	
"Text",
replace("Net due date",'/','') AS "Net due date",
replace("Clearing date",'/','') AS "Clearing date",
"Arrears after net due date" 
--"Reversed with"
from s3dataschema.ods_Finance_FPA_ARBAD_FBL3N 
where Filepath<>'filename' and Customer is not null
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
    exchange_rate_USD
from temp_exchangerate
where fiscal_year_end_date=flag
) d
on 1=1
--on b.calendar_date between d.fifiscal_year_begin_date and d.fiscal_year_end_date
;


create temporary table temp_BAD_Detail AS
select
b.fiscal_year_month_id,
b.fiscal_year_week_id,
"Entity code",
"BU",
"Request Date",
"Customer code",
"Remitter",
"TE Customer",
"TE Customer Chinese",	
"Draft Issuing Bank",	
"Branch Name Chinese",	
"Approved Bank",	
"Draft Amount",	
c.exchange_rate_USD,
"Draft Issue date",	
"Draft Due Date",	
"Draft Number",	
"Draft type",	
"Requestor Name",	
"Number of Endorsement",	
"Number of Original Declaration",	
"Request Status",	
"Number of Declaration",	
"Approval request number",	
"Acknowledgement Date",	--key
"SAP document",
"Received comment"
from 
(
select
"Entity code",
"BU",
"Request Date",
"Customer code",
"Remitter",
"TE Customer",
"TE Customer Chinese",	
"Draft Issuing Bank",	
"Branch Name Chinese",	
"Approved Bank",	
"Draft Amount",	
"Draft Issue date",	
"Draft Due Date",	
"Draft Number",	
"Draft type",	
"Requestor Name",	
"Number of Endorsement",	
"Number of Original Declaration",	
"Request Status",	
"Number of Declaration",	
"Approval request number",	
replace("Acknowledgement Date",'/','') as "Acknowledgement Date",	--key
"SAP document",
"Received comment"
from s3dataschema.ods_Finance_FPA_ARBAD_BADDetail
where Filepath<>'filename'
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
on a."Acknowledgement Date"=b.calendar_date

left join
(
select 
distinct
    fifiscal_year_begin_date ,
	fiscal_year_end_date,
    exchange_rate_USD
from temp_exchangerate
where fiscal_year_end_date=flag
) c
on 1=1
--on b.calendar_date between c.fifiscal_year_begin_date and c.fiscal_year_end_date
;

drop table temp_exchangerate;

--maturity BAD
delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADmaturity;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADmaturity
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

--maturity BAD for APAC
delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADmaturity_APAC;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADmaturity_APAC
(
due_fiscal_year_month,
fiscal_year_month_id,
"Company Code",
Customer,
Bad_Account_Occur_Amount,
timestamps
)
select 
due_fiscal_year_month,
fiscal_year_month_id,
"Company Code",
Customer,
sum( case when "Posting Key"='09' then cast("Amount in local currency" as float)/exchange_rate_USD else 0 end) as Bad_Account_Occur_Amount,
current_timestamp as timestamps
from temp_BAD_Account 
where fiscal_year_month_id is not null and Industry is not null and ("Clearing date"='' or left("Clearing date",8)>=left("Net due date",8))
group by 
fiscal_year_month_id,
due_fiscal_year_month,
"Company Code",
Customer;

--GAP 
delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADAdjustment;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADAdjustment
(
fiscal_year_month_id,
fiscal_year_week_id,
"Company Code",
"Customer code",
Bad_Account_Occur_Amount,
Bad_Account_Collection_Amount,
Bad_Amount_detail,
Adjustment,
timestamps
)
select 
coalesce(a.fiscal_year_month_id,b.fiscal_year_month_id) as fiscal_year_month_id,
coalesce(a.fiscal_year_week_id,b.fiscal_year_week_id) as fiscal_year_week_id,
coalesce(a."Company Code",b."Company Code") as "Company Code",
coalesce(a.Customer,b."Customer code") as "Customer code",
a.Bad_Account_Occur_Amount,
a.Bad_Account_Collection_Amount*-1 as Bad_Account_Collection_Amount,
b.Bad_Amount_detail,
coalesce(a.Bad_Account_Occur_Amount,0)-coalesce(b.Bad_Amount_detail,0) as Adjustment,
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
) a 


full outer join
(
select 
fiscal_year_month_id,
fiscal_year_week_id,
"Entity code" as "Company Code",
"Customer code",
sum("Draft Amount"/exchange_rate_USD) as Bad_Amount_detail
from temp_BAD_Detail
where fiscal_year_month_id is not null
group by 
fiscal_year_month_id,
fiscal_year_week_id,
"Entity code",
"Customer code"
) b
on a.fiscal_year_month_id=b.fiscal_year_month_id and a.fiscal_year_week_id=b.fiscal_year_week_id and 
a."Company Code"=b."Company Code" and a.Customer=b."Customer code";

drop table temp_BAD_Account;
drop table temp_BAD_Detail;

--
delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail
(
fiscal_year_month_id,
fiscal_year_week_id,
due_fiscal_year_month_id,
due_fiscal_year_week_id,
"Entity code",
"BU",
"Request Date",
"Customer code",
"Remitter",
"TE Customer",
"TE Customer Chinese",	
"Draft Issuing Bank",	
"Branch Name Chinese",	
"Approved Bank",	
"Draft Amount",	
"Draft Issue date",	
"Draft Due Date",	
"Draft Number",	
"Draft type",	
"Requestor Name",	
"Number of Endorsement",	
"Number of Original Declaration",	
"Request Status",	
"Number of Declaration",	
"Approval request number",	
"Acknowledgement Date",	--key
"SAP document",
"Received comment",
timestamps
)
select
b.fiscal_year_month_id,
b.fiscal_year_week_id,
c.due_fiscal_year_month_id,
c.due_fiscal_year_week_id,
"Entity code",
"BU",
"Request Date",
"Customer code",
"Remitter",
"TE Customer",
"TE Customer Chinese",	
"Draft Issuing Bank",	
"Branch Name Chinese",	
"Approved Bank",	
"Draft Amount",	
"Draft Issue date",	
"Draft Due Date",	
"Draft Number",	
"Draft type",	
"Requestor Name",	
"Number of Endorsement",	
"Number of Original Declaration",	
"Request Status",	
"Number of Declaration",	
"Approval request number",	
"Acknowledgement Date",	--key
"SAP document",
"Received comment",
CURRENT_TIMESTAMP as timestamps
from 
(
select
"Entity code",
"BU",
"Request Date",
"Customer code",
"Remitter",
"TE Customer",
"TE Customer Chinese",	
"Draft Issuing Bank",	
"Branch Name Chinese",	
"Approved Bank",	
"Draft Amount",	
"Draft Issue date",	
replace("Draft Due Date",'/','') as "Draft Due Date",	
"Draft Number",	
"Draft type",	
"Requestor Name",	
"Number of Endorsement",	
"Number of Original Declaration",	
"Request Status",	
"Number of Declaration",	
"Approval request number",	
replace("Acknowledgement Date",'/','') as "Acknowledgement Date",	--key
"SAP document",
"Received comment"
from s3dataschema.ods_Finance_FPA_ARBAD_BADDetail
where Filepath<>'filename'
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
on a."Acknowledgement Date"=b.calendar_date

left join 
(
select 
distinct
fiscal_year_month_id as due_fiscal_year_month_id,
fiscal_year_week_id as due_fiscal_year_week_id,
to_char(calendar_date,'YYYYMMDD') as calendar_date
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_id>=2024
) c
on a."Draft Due Date"=c.calendar_date;

create temporary table Temp_Baddetail as 
select
fiscal_year_month_id,
fiscal_year_week_id,
due_fiscal_year_month_id,
due_fiscal_year_week_id,
"Entity code",
"Customer code",
"Draft Issuing Bank",	
"Branch Name Chinese",	
sum("Draft Amount") as "Draft Amount"
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail
group by 
fiscal_year_month_id,
fiscal_year_week_id,
due_fiscal_year_month_id,
due_fiscal_year_week_id,
"Entity code",
"Customer code",
"Draft Issuing Bank",	
"Branch Name Chinese";


delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADEnd;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADEnd
(
fiscal_year_month_id,
fiscal_year_week_id,
fiscal_year_month_id_2,
fiscal_year_week_id_2,
due_fiscal_year_month_id,
due_fiscal_year_week_id,
"Entity code",
"Customer code",
"Draft Issuing Bank",	
"Branch Name Chinese",	
"Draft Amount",
timestamps
)
select
a.fiscal_year_month_id,
a.fiscal_year_week_id,
b.fiscal_year_month_id as fiscal_year_month_id_2,
b.fiscal_year_week_id as fiscal_year_week_id_2,
b.due_fiscal_year_month_id,
b.due_fiscal_year_week_id,
b."Entity code",
b."Customer code",
b."Draft Issuing Bank",	
b."Branch Name Chinese",	
b."Draft Amount",
CURRENT_TIMESTAMP as timestamps
from 
(
select
distinct
fiscal_year_month_id,
fiscal_year_week_id
from Temp_Baddetail
) a 

left join 

(
select
fiscal_year_month_id,
fiscal_year_week_id,
due_fiscal_year_month_id,
due_fiscal_year_week_id,
"Entity code",
"Customer code",
"Draft Issuing Bank",	
"Branch Name Chinese",	
"Draft Amount"
from Temp_Baddetail
) b
ON a.fiscal_year_week_id<b.due_fiscal_year_week_id and a.fiscal_year_week_id>=b.fiscal_year_week_id;

drop table Temp_Baddetail;


END;
$$;

select sum(Bad_Account_Occur_Amount)/1000000 from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADmaturity 
where due_fiscal_year_month=202602


select
sum(Bad_Account_Occur_Amount)
from
(
select 
"Net due date",
"Clearing date",
sum( case when "Posting Key"='09' then cast("Amount in local currency" as float)/exchange_rate_USD else 0 end) /1000000 as Bad_Account_Occur_Amount,
current_timestamp as timestamps
from temp_BAD_Account 
where fiscal_year_month_id is not null and Industry is not null and ("Clearing date"='' or left("Clearing date",8)>=left("Net due date",8)) and due_fiscal_year_month=202602
group by 
"Net due date",
"Clearing date"

) a
