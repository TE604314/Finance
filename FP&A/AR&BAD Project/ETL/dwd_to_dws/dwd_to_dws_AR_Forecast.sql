call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_ARBAD_DWD_to_DWS_ARForecast()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_ARBAD_DWD_to_DWS_ARForecast()
LANGUAGE plpgsql
AS $$
BEGIN


create temporary table Temp_Forecast_AR_OutstandingBAD as 
select 
a.fiscal_yearmonth,
a.fiscal_week,
a."Company Code",
"Customer_Code",	
AR_Due_amount,
b."BAD discount amount",
b.Monthly_BADDiscount,
b.WTD_BADDiscount_OneMonth,
b.Monthly_BADDiscount-b.WTD_BADDiscount_OneMonth as Outstanding_BAD,
"Period BAD Weight",
"Flag",
"Longtime_BAD",
PastdueARPercentage_rate,
Advanced_Payment,
"BAD_Discount In advance"
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_Forecast_AR a


left join 

(
select
due_fiscal_year_month_id,
due_fiscal_year_week_id,
"Company code",
"Customer code",	
"BAD discount amount",
sum("BAD discount amount") over(partition by due_fiscal_year_month_id,"Company code","Customer code") as Monthly_BADDiscount,
sum("BAD discount amount") over(partition by due_fiscal_year_month_id,"Company code","Customer code" order by due_fiscal_year_week_id 
rows between unbounded  preceding and CURRENT ROW) as WTD_BADDiscount_OneMonth
from
	(
	select
	due_fiscal_year_month_id,
	due_fiscal_year_week_id,
	"Entity code"  as "Company code",
	"Customer code",	
	cast(sum("Draft Amount") as decimal(25,10)) as "BAD discount amount"
	from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail
	group by 
	due_fiscal_year_month_id,
	due_fiscal_year_week_id,
	"Company code",
	"Customer code"
	) b1
) b 
on a.fiscal_yearmonth=b.due_fiscal_year_month_id and a.Customer_Code=b."Customer code" and a."Company code"=b."Company code" and a.fiscal_week=b.due_fiscal_year_week_id;
--forecast

create temporary table BAD_YTD as 
select 
fiscal_year,
fiscal_year_month_id,
"Company code",
"Customer code",	
"Draft Amount",
--sum("Draft Amount") over(partition by fiscal_year,"Customer code")
sum("Draft Amount") over
(partition by fiscal_year,"Company code","Customer code"  order by fiscal_year_month_id rows between unbounded  preceding and CURRENT ROW) as YTD_Amount
from
(
select
left(fiscal_year_month_id,4) as fiscal_year,
fiscal_year_month_id,
"Customer code",	
"Entity code"  as "Company code",
sum("Draft Amount") as "Draft Amount"
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_BADDetail
group by 
fiscal_year_month_id,
"Entity code",
"Customer code"
) a 

inner join

(
select 
Customer_Code,
max("Flag") as Flag
from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_ARBAD_CustomerList
where flag<>'Ratio'
group by Customer_Code
) b
on a."Customer code"=b.Customer_Code;


create temporary table temp_Forecast_AR as 
select 
fiscal_yearmonth,
fiscal_week,
"Company Code",
"Customer_Code",	
AR_Due_amount,
"BAD discount amount",
Monthly_BADDiscount,
WTD_BADDiscount_OneMonth,
Outstanding_BAD,
case when flag is null then 0 else (coalesce(AR_Due_amount,0)-coalesce(Outstanding_BAD,0))*"Period BAD Weight" end as "BAD receive amount(assumed)",
"Period BAD Weight",
"Flag",
"Longtime_BAD",
PastdueARPercentage_rate,
Advanced_Payment,
"BAD_Discount In advance",
0 as Monthly_Amount,
0 as YTD_Amount
from Temp_Forecast_AR_OutstandingBAD
where Flag='Ratio' or Flag is null 

union all

select 
fiscal_yearmonth,
fiscal_week,
"Company code",
"Customer_Code",	
AR_Due_amount,
"BAD discount amount",
Monthly_BADDiscount,
WTD_BADDiscount_OneMonth,
Outstanding_BAD,
case when "Period BAD Weight">=COALESCE(YTD_Amount,0)+(coalesce(AR_Due_amount,0)-coalesce(Outstanding_BAD,0)) then (coalesce(AR_Due_amount,0)-coalesce(Outstanding_BAD,0)) else
     case when "Period BAD Weight">=COALESCE(YTD_Amount,0) then "Period BAD Weight"-COALESCE(YTD_Amount,0) else
          0
     end
end as "BAD receive amount(assumed)",
"Period BAD Weight",
"Flag",
"Longtime_BAD",
PastdueARPercentage_rate,
Advanced_Payment,
"BAD_Discount In advance",
0 as Monthly_Amount,
b.YTD_Amount
from 
(
select 
fiscal_yearmonth,
fiscal_week,
"Company Code",
"Customer_Code",	
AR_Due_amount,
"BAD discount amount",
Monthly_BADDiscount,
WTD_BADDiscount_OneMonth,
Outstanding_BAD,
"Period BAD Weight",
"Flag",
"Longtime_BAD",
PastdueARPercentage_rate,
Advanced_Payment,
"BAD_Discount In advance"
from Temp_Forecast_AR_OutstandingBAD
where Flag ='Value Yearly'
) a

left join 
(
select 
fiscal_year,
fiscal_year_month_id,
"Customer code",
"Draft Amount",
YTD_Amount
from BAD_YTD
) b
on a.fiscal_yearmonth=b.fiscal_year_month_id and a."Customer_Code"=b."Customer code"

union all

select 
fiscal_yearmonth,
fiscal_week,
"Company code",
"Customer_Code",	
AR_Due_amount,
"BAD discount amount",
Monthly_BADDiscount,
WTD_BADDiscount_OneMonth,
Outstanding_BAD,
case when "Period BAD Weight">=COALESCE(Monthly_Amount,0)+(coalesce(AR_Due_amount,0)-coalesce(Outstanding_BAD,0)) then (coalesce(AR_Due_amount,0)-coalesce(Outstanding_BAD,0)) else
     case when "Period BAD Weight">=COALESCE(Monthly_Amount,0) then "Period BAD Weight"-(coalesce(AR_Due_amount,0)-coalesce(Outstanding_BAD,0)) else
          0
     end
end as "BAD receive amount(assumed)",
"Period BAD Weight",
"Flag",
"Longtime_BAD",
PastdueARPercentage_rate,
Advanced_Payment,
"BAD_Discount In advance",
b.Monthly_Amount,
0 as YTD_Amount
from 
(
select 
fiscal_yearmonth,
fiscal_week,
"Company Code",
"Customer_Code",	
AR_Due_amount,
"BAD discount amount",
Monthly_BADDiscount,
WTD_BADDiscount_OneMonth,
Outstanding_BAD,
"Period BAD Weight",
"Flag",
"Longtime_BAD",
PastdueARPercentage_rate,
Advanced_Payment,
"BAD_Discount In advance"
from Temp_Forecast_AR_OutstandingBAD
where Flag ='Value Monthly'
) a

left join 
(
select 
fiscal_year,
fiscal_year_month_id,
"Customer code",
"Draft Amount" as Monthly_Amount
from BAD_YTD
) b
on a.fiscal_yearmonth=b.fiscal_year_month_id and a."Customer_Code"=b."Customer code";

drop table BAD_YTD;
drop table Temp_Forecast_AR_OutstandingBAD;

delete ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_ARBAD_Forecast_AR;
insert into ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_ARBAD_Forecast_AR
(
fiscal_yearmonth,
fiscal_week,
"Company code",
"Customer_Code",	
AR_Due_amount,
"BAD receive amount(assumed)",
"AR due amount cash",
"AR collect amount(assumed)",
"BAD discount amount",
Monthly_BADDiscount,
WTD_BADDiscount_OneMonth,
Outstanding_BAD,
"Period BAD Weight",
"Flag",
"Longtime_BAD",
PastdueARPercentage_rate,
Advanced_Payment,
"BAD_Discount In advance",
Monthly_Amount,
YTD_Amount,
timestamps
)
select 
fiscal_yearmonth,
fiscal_week,
"Company code",
"Customer_Code",	
AR_Due_amount,
"BAD receive amount(assumed)",
coalesce(AR_Due_amount,0)-coalesce(Outstanding_BAD,0)-coalesce("BAD receive amount(assumed)",0) as "AR due amount cash",
(coalesce(AR_Due_amount,0)-coalesce(Outstanding_BAD,0)-coalesce("BAD receive amount(assumed)",0))*(1-PastdueARPercentage_rate) as "AR collect amount(assumed)",
"BAD discount amount",
Monthly_BADDiscount,
WTD_BADDiscount_OneMonth,
Outstanding_BAD,
"Period BAD Weight",
"Flag",
"Longtime_BAD",
PastdueARPercentage_rate,
Advanced_Payment,
"BAD_Discount In advance",
Monthly_Amount,
YTD_Amount,
current_timestamp as timestamps
from 
(
select 
fiscal_yearmonth,
fiscal_week,
"Company code",
"Customer_Code",	
cast(AR_Due_amount as float) as AR_Due_amount,
cast("bad discount amount" as float) as "bad discount amount",
cast(Monthly_BADDiscount as float) as Monthly_BADDiscount,
cast(WTD_BADDiscount_OneMonth as float) as WTD_BADDiscount_OneMonth,
cast(Outstanding_BAD as float) as Outstanding_BAD,
cast("BAD receive amount(assumed)" as float) as "BAD receive amount(assumed)",
cast(AR_Due_amount as float)-cast(Outstanding_BAD as float)-cast("BAD receive amount(assumed)" as float)  as "AR due amount cash",
(cast(AR_Due_amount as float)-cast(Outstanding_BAD as float)-cast("BAD receive amount(assumed)" as float))*(1-PastdueARPercentage_rate)  as "AR collect amount(assumed)",
cast("Period BAD Weight" as  float) as "Period BAD Weight",
"Flag",
"Longtime_BAD",
cast(PastdueARPercentage_rate as float) as PastdueARPercentage_rate,
cast(Advanced_Payment as float) as Advanced_Payment,
cast("BAD_Discount In advance" as float) as "BAD_Discount In advance",
cast(Monthly_Amount as float) as Monthly_Amount,
cast(YTD_Amount as float) as YTD_Amount
from temp_Forecast_AR
) a ;



drop table temp_Forecast_AR;

END;
$$;