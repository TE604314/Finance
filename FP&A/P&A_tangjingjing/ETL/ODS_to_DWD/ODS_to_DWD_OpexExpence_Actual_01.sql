call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Opex_ODS_to_DWD_OpexExpenceActual()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Opex_ODS_to_DWD_OpexExpenceActual()
LANGUAGE plpgsql
AS $$
BEGIN



delete ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_CostCenterInfo;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_CostCenterInfo 
(
cost_center_effective_from_date,
cost_center_effective_to_date,
cost_center_company_code,
cost_center_department_number,
cost_center_building_country_code,
trimmed_cost_center_id,
cost_center_profit_center_id,
cost_center_category_code,
cost_center_name,
timesatamps
)
select
to_date(cost_center_effective_from_date,'YYYY-MM-DD') as cost_center_effective_from_date,
to_date(cost_center_effective_to_date,'YYYY-MM-DD') as cost_center_effective_to_date,
cost_center_company_code,
cost_center_department_number,
cost_center_building_country_code,
trimmed_cost_center_id,
cost_center_profit_center_id,
cost_center_category_code,
cost_center_name,
current_timestamp as timesatamps
from master_data.dimension_cost_center
where  cost_center_status_code='C' and left(cost_center_effective_to_date,4)>=2015;



delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12Rawdata;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12Rawdata
(
Fyear,
Account,
Description,
AccountDescription,
CC_Flag,
CostCenter,
CostCenter2, 
Rowno,
"Per 1",
"Per 2",
"Per 3",
"Per 4",
"Per 5",
"Per 6",
"Per 7",
"Per 8",
"Per 9",
"Per 10",
"Per 11",
"Per 12",
timestamps
)
select
a.Fyear,
Account,
Description,
AccountDescription,
b.CC_Flag,
case when b.CC_Flag=1 then Account else null end as CostCenter,
first_value((case when b.CC_Flag=1 then Account else null end ) ignore nulls)over(partition by a.Fyear order by Rowno asc rows between current row and unbounded following ) as CostCenter2, 
Rowno,
"Per 1",
"Per 2",
"Per 3",
"Per 4",
"Per 5",
"Per 6",
"Per 7",
"Per 8",
"Per 9",
"Per 10",
"Per 11",
"Per 12",
current_timestamp as timestamps
from 
(
select 
upper(right(split_part(Filename,'.x',1),4)) as Fyear,
trim(split_part(AccountDescription,'  ',1)) as Account,
ltrim(replace(AccountDescription,split_part(AccountDescription,'  ',1),'')) as Description,
AccountDescription,
nullif(trim("Per 1"), '')  as "Per 1",
nullif(trim("Per 2"), '')  as "Per 2",
nullif(trim("Per 3"), '')  as "Per 3",
nullif(trim("Per 4"), '')  as "Per 4",
nullif(trim("Per 5"), '')  as "Per 5",
nullif(trim("Per 6"), '')  as "Per 6",
nullif(trim("Per 7"), '')  as "Per 7",
nullif(trim("Per 8"), '')  as "Per 8",
nullif(trim("Per 9"), '')  as "Per 9",
nullif(trim("Per 10"), '')  as "Per 10",
nullif(trim("Per 11"), '')  as "Per 11",
nullif(trim("Per 12"), '')  as "Per 12",
row_number()over(partition by right(split_part(Filename,'.x',1),4)) as Rowno
from s3dataschema.ods_Finance_FPA_Opex_ZB12 
where "Per 1"<>'Per 1'  and AccountDescription is not null and AccountDescription<>''
) a 


left join 

(
select
distinct
trimmed_cost_center_id as "Cost Center",
1 as CC_Flag
from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_CostCenterInfo
) b
on a.Account=b."Cost Center";

create temporary table Temp_TBR AS 
select 
    1 as id,
    left(to_char(expiration_dt,'YYYYMMDD'),4) as fifiscal_year,
    left(to_char(expiration_dt,'YYYYMMDD'),8) as fifiscal_year_begin_date ,
	left(to_char(expiration_dt,'YYYYMMDD'),8) as fiscal_year_end_date,
    max(left(to_char(expiration_dt,'YYYYMMDD'),8)) over(partition by 1) as flag,
    currency_cde,
    "rate_from_us_mult_fctr" as exchange_rate_USD
FROM "ss_rs_ts_aut_ap_digital_db"."master_data"."dimension_currency_exchange_rates_all"
WHERE currency_exchange_rate_cde=1 and currency_cde in ('CNY','TWD','USD') and 
left(to_char(expiration_dt,'YYYYMMDD'),4) in (select max(replace(Fyear,'FY','20')) from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12Rawdata);


/*
left join 

(
select
distinct
Fyear,
"Cost Center",
1 as CC_Flag
from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_KS13
) b 
on a.Fyear=b.Fyear and a.Account=b."Cost Center";
*/

create temporary table Temp_ZB12 as 
select 
1 as ID,
replace(Fyear,'FY','20') as Year,
concat(replace(Fyear,'FY','20'),right(Months,2)) as YearMonth,
CostCenter2,
Account,
Description,
AccountDescription,
Months,
Amount,
current_timestamp as timestamps
from 
(
select
Fyear,
CostCenter2,
Account,
Max(Description) as Description,
Max(AccountDescription) as AccountDescription,
sum("Per 1") as "P01",
sum("Per 2") as "P02",
sum("Per 3") as "P03",
sum("Per 4") as "P04",
sum("Per 5") as "P05",
sum("Per 6") as "P06",
sum("Per 7") as "P07",
sum("Per 8") as "P08",
sum("Per 9") as "P09",
sum("Per 10") as "P10",
sum("Per 11") as "P11",
sum("Per 12") as "P12"
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12Rawdata
where coalesce(CC_Flag,'0')<>'1'
group by 
Fyear,
CostCenter2,
Account) a 
unpivot(Amount for Months in ("P01","P02","P03","P04","P05","P06","P07","P08","P09","P10","P11","P12"));


create temporary table Temp_ZB12_final as 
select 
Year,
YearMonth,
c."location",
CostCenter2,
b.cost_center_category_code,
e."Department",
e."Team",
Account,
Description,
AccountDescription,
Months,
cny_usd_rate,
twd_usd_rate,
Amount
from Temp_ZB12 a 

left join 

(
select 
trimmed_cost_center_id,
max(cost_center_company_code) as cost_center_company_code,
max(cost_center_category_code) as cost_center_category_code
from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_CostCenterInfo
group by trimmed_cost_center_id
)  b
on a.CostCenter2=b.trimmed_cost_center_id

left join
(
Select 
"Company code",
max("location") as "location"
FROM  s3dataschema.ods_Finance_FPA_Opex_CompanyInfo
group by "Company code"
) c 
on b.cost_center_company_code=c."Company code"

left join 

(
select
1 AS ID,
sum(cny_usd_rate) as cny_usd_rate,
sum(twd_usd_rate) as twd_usd_rate
from
(
select 
--fifiscal_year,
 1 AS ID,
 case when currency_cde='CNY' then exchange_rate_USD else 0 end  AS cny_usd_rate,
 case when currency_cde='TWD' then exchange_rate_USD else 0 end AS twd_usd_rate
 from Temp_TBR
 ) a1
 group by ID
) d
on a.ID=d.ID

left join
(
select 
"Function area",
max("Department") as "Department",
max("Team") as "Team"
from s3dataschema.ods_Finance_FPA_Opex_FuctionArea
where "Team"<>'Team'
group by 
"Function area"
) e
on b.cost_center_category_code=e."Function area"
;

drop table Temp_ZB12;

delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12
(
Year,
YearMonth,
"location",
CostCenter2,
cost_center_category_code,
"Department",
"Team",
Account,
Description,
AccountDescription,
Months,
cny_usd_rate,
twd_usd_rate,
Original_Amount,
Amount_CNY,
timestamps
)
select 
Year,
YearMonth,
"location",
CostCenter2,
cost_center_category_code,
"Department",
"Team",
Account,
Description,
AccountDescription,
Months,
cny_usd_rate,
twd_usd_rate,
Amount as Original_Amount,
case when "location"='HK' then amount*cny_usd_rate
     when "location"='TW' then amount/twd_usd_rate*cny_usd_rate ELSE
      Original_amount END AS Amount_CNY,
current_timestamp as timestamps
from Temp_ZB12_final
where coalesce(Amount,0)<>0;


drop table Temp_ZB12_final;


--KE5Z --内部成本转移
create temporary table KE5Z_Temp as 
select
Fiscal_Yearmonth,
"Receiver CoCde",	
"Ref. Document",	
"Fiscal Year",	
"Posting period",	
a."Profit Center",	
"Account Number",	
"In company code currency",	
"Entry Date",	
"Posting Date",	
"Quantity",	
"Cost Center",	
"Functional Area",	
"WBS Element",
"PRJ NO",
b.BU,
b.BU||' '||"Functional Area" as BU_Function,
c."location",
c1.Department,
c1.Team,
d.ProductLine,
d."Project Name"
from
(
select
"Fiscal Year"||case when coalesce(cast("Posting period" AS INT),0)<10 then '0'||"Posting period" else "Posting period" end as Fiscal_Yearmonth,
"Receiver CoCde",	
"Ref. Document",	
"Fiscal Year",	
"Posting period",	
"Profit Center",	
"Account Number",	
"In company code currency",	
"Entry Date",	
"Posting Date",	
"Quantity",	
"Cost Center",	
"Functional Area",	
"WBS Element",
'PRJ-'||substring("WBS Element",3,2)||'-000'||substring("WBS Element",5,6) as "PRJ NO"
from s3dataschema.ods_Finance_FPA_Opex_KE5Z
where "Fiscal Year"<>'Fiscal Year'  and "Ref. Document"<>'' and "Ref. Document" is not null
) a

left join

(
select
"Profit center",
max(case when "BU"='others' then 'Others' else "BU" end) as BU
from s3dataschema.ods_Finance_FPA_Opex_ProfitCenter
group by "Profit center"
) b
on a."Profit Center"=b."Profit center"

left join 

(
select
"Company code",
max("location") as "location"
from  s3dataschema.ods_Finance_FPA_Opex_CompanyInfo
group by "Company code"
) c
on a."Receiver CoCde"=c."Company code"

left join
 
(
select
"Function area",
max("Department") as Department,
max("Team") as Team
from  s3dataschema.ods_Finance_FPA_Opex_FuctionArea
group by 
"Function area"
) c1
on a."Functional Area"=c1."Function area"

left join 

(
select
"Project Number",
max("ProductLine") as ProductLine,
max("Project Name") as "Project Name"
from s3dataschema.ods_Finance_FPA_Opex_projectlist
group by "Project Number"
) d 
on 'PRJ-'||substring(a."WBS Element",3,2)||'-000'||substring(a."WBS Element",5,6)=d."Project Number";

delete ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_Engineeringallocation;
insert into ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_Engineeringallocation
(
Fiscal_Yearmonth,
Companycode,	
"Ref. Document",	
"Fiscal Year",	
"Posting period",	
"Profit Center",	
"Account Number",	
"In company code currency",	
"Entry Date",	
"Posting Date",	
Quantity,	
"Cost Center",	
"Functional Area",	
"WBS Element",
"PRJ NO",
BU,
BU_Function,
"location",
Department,
Team,
"To_In company code currency",		
To_Quantity,		
"To_WBS Element",
"To_PRJ NO",
"To_BU",
"To_BU_Function",
"To_location",
"To_Department",
"To_Team",
To_Companycode,
"To_Profit Center",
"To_Functional Area",
"To_ProductLine",
"To_Project Name",
Phase,
cny_usd_rate,
twd_usd_rate,
"In company code currency CNY",
"To_In company code currency CNY",
Comments,
timestamps
)
select
a.Fiscal_Yearmonth,
a.Companycode,	
a."Ref. Document",	
"Fiscal Year",	
"Posting period",	
"Profit Center",	
"Account Number",	
coalesce(cast("In company code currency" as decimal(20,10)),0) as "In company code currency",	
"Entry Date",	
"Posting Date",	
coalesce(cast("Quantity" as decimal(20,10)),0)  as Quantity,	
"Cost Center",	
"Functional Area",	
"WBS Element",
"PRJ NO",
BU,
BU_Function,
"location",
Department,
Team,
coalesce(cast("To_In company code currency" as decimal(20,10)),0) as "To_In company code currency",		
coalesce(cast("To_Quantity" as decimal(20,10)),0) as To_Quantity,		
"To_WBS Element",
"To_PRJ NO",
"To_BU",
"To_BU_Function",
"To_location",
To_Department,
To_Team,
To_Companycode,
"To_Profit Center",
"To_Functional Area",
"To_ProductLine",
"To_Project Name",
substring("To_WBS Element",18,1) as Phase,
c.cny_usd_rate,
c.twd_usd_rate,
case when "location"='HK' THEN coalesce(cast("In company code currency" as decimal(20,10)),0)*c.cny_usd_rate
     when "location"='TW' then coalesce(cast("In company code currency" as decimal(20,10)),0)/c.twd_usd_rate*c.cny_usd_rate else 
coalesce(cast("In company code currency" as decimal(20,10)),0) end as "In company code currency CNY",
case when "location"='HK' THEN coalesce(cast("To_In company code currency" as decimal(20,10)),0)*c.cny_usd_rate
     when "location"='TW' then coalesce(cast("To_In company code currency" as decimal(20,10)),0)/c.twd_usd_rate*c.cny_usd_rate else 
coalesce(cast("To_In company code currency" as decimal(20,10)),0) end as "To_In company code currency CNY",
BU_Function||' Charge to '||To_BU_Function as Comments,
current_timestamp as timestamps
from 
(
select
1 as ID,
Fiscal_Yearmonth,
"Receiver CoCde" as Companycode,	
"Ref. Document",	
"Fiscal Year",	
"Posting period",	
"Profit Center",	
"Account Number",	
"In company code currency",	
"Entry Date",	
"Posting Date",	
"Quantity",	
"Cost Center",	
"Functional Area",	
"WBS Element",
"PRJ NO",
BU,
BU_Function,
"location",
Department,
Team
--ProductLine
from KE5Z_Temp
where ("WBS Element" is null or "WBS Element"='') and "Ref. Document" is not null and "Ref. Document"<>''
) a 

left join 

(
select
Fiscal_Yearmonth,
"Ref. Document",
"Receiver CoCde" as To_Companycode,
"In company code currency" as "To_In company code currency",		
"Quantity" as "To_Quantity",	
"WBS Element" as "To_WBS Element",
"PRJ NO" as "To_PRJ NO",
BU as "To_BU",
BU_Function as "To_BU_Function",
"Profit Center" as "To_Profit Center",
"location" as "To_location",
Department as To_Department,
"Functional Area" as "To_Functional Area",
Team as To_Team,
ProductLine as "To_ProductLine",
"Project Name" as "To_Project Name"
from KE5Z_Temp
where "WBS Element" is not null and "WBS Element"<>''
) b 
on a.Fiscal_Yearmonth=b.Fiscal_Yearmonth and a."Ref. Document"=b."Ref. Document"


left join 

(
select
1 AS ID,
sum(cny_usd_rate) as cny_usd_rate,
sum(twd_usd_rate) as twd_usd_rate
from
(
select 
--fifiscal_year,
 1 AS ID,
 case when currency_cde='CNY' then exchange_rate_USD else 0 end  AS cny_usd_rate,
 case when currency_cde='TWD' then exchange_rate_USD else 0 end AS twd_usd_rate
 from Temp_TBR
 ) a1
 group by ID
) c
on a.ID=c.ID;



--combine Actual
delete  ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual;
insert into ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
(
fisical_Year,
fisical_YearMonth,
"location",
CostCenter2,
cost_center_category_code,
"Department",
BU_Function,
"Team",
Account,
Description,
AccountDescription,
Months,
cny_usd_rate,
twd_usd_rate,
Original_Amount,
Amount_CNY,
timestamps
)
select
Year as fisical_Year,
YearMonth as fisical_YearMonth,
"location",
CostCenter2,
cost_center_category_code,
"Department",
'ZB12 for identify E Allo' as BU_Function,
"location"||' '||"Team" as "Team",
Account,
Description,
AccountDescription,
Months,
cny_usd_rate,
twd_usd_rate,
Original_Amount,
Amount_CNY,
current_timestamp as timestamps
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12 

union all

--charge in
select
"Fiscal Year" as fisical_Year,
Fiscal_Yearmonth,
"location",
"Cost Center" as CostCenter2,
"Functional Area" as cost_center_category_code,
"Department",
BU_Function,
"location"||' '||"Team" as "Team",
"Account Number" as Account,
case when "Account Number"='9000001' then '9000001  Engineering cost center-labor'
when "Account Number"='9000002' then '9000002  Engineering-overhead' else 'Others' end
as Description, 
case when "Account Number"='9000001' then 'In 9000001  Engineering cost center-labor'
when "Account Number"='9000002' then 'In 9000002  Engineering-overhead' else 'Others' end
as AccountDescription,
right(Fiscal_Yearmonth,2) as Months,
cny_usd_rate,
twd_usd_rate,
"In company code currency" as Original_Amount,
"In company code currency CNY" as Amount_CNY,
current_timestamp as timestamps
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_Engineeringallocation


union all

--charge to team
select
"Fiscal Year" as fisical_Year,
Fiscal_Yearmonth,
To_location as "location",
"To_Functional Area"||'_'||To_Companycode||'_'||"To_Profit Center" as CostCenter2,
"Functional Area" as cost_center_category_code,
"To_Department" as "Department",
"To_BU_Function" as BU_Function,
"To_location"||' '||"To_Team" as "Team",
"Account Number" as Account,	
case when "Account Number"='9000001' then '9000001  Engineering cost center-labor'
when "Account Number"='9000002' then '9000002  Engineering-overhead' else 'Others' end
as Description,
case when "Account Number"='9000001' then 'to 9000001  Engineering cost center-labor'
when "Account Number"='9000002' then 'to 9000002  Engineering-overhead' else 'Others' end
as AccountDescription,
right(Fiscal_Yearmonth,2) as Months,
cny_usd_rate,
twd_usd_rate,
"To_In company code currency" as Original_Amount,
"To_In company code currency CNY" as Amount_CNY,
current_timestamp as timestamps
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_Engineeringallocation;



--Actual HC
create temporary table Actual_HC AS
select
F_year||right(cast(replace(lower(SPLIT_PART(Months,'-',2)),'p','') as int)+100,2) as F_YM,
"Cost Center",
Name,
Region,
"Company Code",
"Profit Center",
"Person Responsible",
Category,
Department,
Team,
Mark,
coalesce(cast(quatity as float),0) as quatity,
current_timestamp as timestamps
from 
(
select 
replace(left(Filename,4),'FY','20') AS F_year,
"Cost Center",
Name,
Region,
"Company Code",
"Profit Center",
"Person Responsible",
Category,
Department,
Team,
Mark,
"HC-P1",
"HC-P2",
"HC-P3",
"HC-P4",
"HC-P5",
"HC-P6",
"HC-P7",
"HC-P8",
"HC-P9",
"HC-P10",
"HC-P11",
"HC-P12"
FROM s3dataschema.ods_Finance_FPA_Opex_ActualHC
where name<>'Name'
) a
unpivot(quatity for Months in ("HC-P1","HC-P2","HC-P3","HC-P4","HC-P5","HC-P6","HC-P7","HC-P8","HC-P9","HC-P10","HC-P11","HC-P12"))
where coalesce(cast(quatity as float),0)<>0 and quatity is not null;

delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Actual_HC;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Actual_HC
(
F_YM,
"Cost Center",
Name,
Region,
"Company Code",
"Profit Center",
"Person Responsible",
Category,
Department,
Team,
Department_new,
Team_new,
Mark,
quatity,
timestamps
)
select
F_YM,
"Cost Center",
Name,
Region,
"Company Code",
"Profit Center",
"Person Responsible",
Category,
a.Department,
a.Team,
c."Department" as Department_new,
Region||' '||c."Team" as Team_new,
Mark,
quatity,
current_timestamp as timestamps
from
(
select
F_YM,
"Cost Center",
Name,
Region,
"Company Code",
"Profit Center",
"Person Responsible",
Category,
Department,
Team,
Mark,
quatity
FROM Actual_HC
) a

left join 

(
select 
trimmed_cost_center_id,
max(cost_center_category_code) as cost_center_category_code
from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_CostCenterInfo
group by trimmed_cost_center_id
)  b
on a."Cost Center"=b.trimmed_cost_center_id

left join 
(
select
"Function area",
max("Department") as "Department",
max("Team") as "Team"
from s3dataschema.ods_Finance_FPA_Opex_FuctionArea
group by "Function area"
) c
on b.cost_center_category_code=c."Function area";

drop table Actual_HC;
drop table KE5Z_Temp;
DROP TABLE Temp_TBR;
END;
$$;
