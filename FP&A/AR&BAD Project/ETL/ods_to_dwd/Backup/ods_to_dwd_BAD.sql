select * from temp_BAD_Account
where fiscal_year_month_id is null

create temporary table temp_BAD_Account AS
select
fiscal_year_month_id,
fiscal_year_week_id,
"Company Code",
"Account",
"Document Number",	
"Plant",
"Assignment",
"Document Date",
"Posting Date",
"Document Type",
"Posting Key",	
"Amount in local currency",	
"Local Currency",	
"Amount in doc. curr",	
"Document currency",	
"Profit Center",	
"Text",	
"Reversed with"
from
(
select 
"Company Code",
"Account",
"Document Number",	
"Plant",
"Assignment",
"Document Date",
replace("Posting Date",'/','') as "Posting Date",
"Document Type",
"Posting Key",	
"Amount in local currency",	
"Local Currency",	
"Amount in doc. curr",	
"Document currency",	
"Profit Center",	
"Text",	
"Reversed with"
from s3dataschema.ods_Finance_FPA_ARBAD_FBL3N 
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
on a."Posting Date"=b.calendar_date;

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
on a."Acknowledgement Date"=b.calendar_date;

--GAP 
delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADAdjustment;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADAdjustment
(
fiscal_year_month_id,
fiscal_year_week_id,
"Company Code",
Bad_Account_Amount,
Bad_Amount_detail,
Adjustment,
timestamps
)
select 
coalesce(a.fiscal_year_month_id,b.fiscal_year_month_id) as fiscal_year_month_id,
coalesce(a.fiscal_year_week_id,b.fiscal_year_week_id) as fiscal_year_week_id,
coalesce(a."Company Code",b."Company Code") as "Company Code",
a.Bad_Account_Amount,
b.Bad_Amount_detail,
coalesce(a.Bad_Account_Amount,0)-coalesce(b.Bad_Amount_detail,0) as Adjustment,
current_timestamp as timestamps
from
(
select 
fiscal_year_month_id,
fiscal_year_week_id,
"Company Code",
sum("Amount in local currency") as Bad_Account_Amount
from temp_BAD_Account 
where fiscal_year_month_id is not null
group by 
fiscal_year_month_id,
fiscal_year_week_id,
"Company Code"
) a 

full outer join
(
select 
fiscal_year_month_id,
fiscal_year_week_id,
"Entity code" as "Company Code",
sum("Draft Amount") as Bad_Amount_detail
from temp_BAD_Detail
where fiscal_year_month_id is not null
group by 
fiscal_year_month_id,
fiscal_year_week_id,
"Entity code"
) b
on a.fiscal_year_month_id=b.fiscal_year_month_id and a.fiscal_year_week_id=b.fiscal_year_week_id and 
a."Company Code"=b."Company Code";

drop table temp_BAD_Account;
drop table temp_BAD_Detail;


delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail
(
fiscal_year_month_id,
fiscal_year_week_id,
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