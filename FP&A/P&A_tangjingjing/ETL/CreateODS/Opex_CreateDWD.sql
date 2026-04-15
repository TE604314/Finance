
drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1
select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1 limit 100

drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactStep1
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactStep1
(
fisical_YearMonth varchar(255),
Department varchar(255),
Team varchar(255),
"Expense level 1" varchar(255),
"Expense level 2" varchar(255),	
"Acc" varchar(255),	
"Accounts/Cost Elements" varchar(255),	
"Logic" varchar(255),	
"Index" varchar(255),	
"Comments" varchar(255),
Manual_Forecast_Adj decimal(25,10),
budget_index2 decimal(25,10),
Budget_ActualAvg_budget_index3 decimal(25,10),
"Budget_Actual_total_index5-6" decimal(25,10),
Budget_finalmonth_Actual_Index8 decimal(25,10),
Budget_ActualAvg_Salesv1_index9 decimal(25,10),
Budget_ActualAvg_Salesv1_index10 decimal(25,10),
Budget_Capexforecast_index7 decimal(25,10),
Budget_leasingforecast_index8 decimal(25,10),
Budget_eachmonth_index12 decimal(25,10),
budget_Allocations_index13 decimal(25,10),
timestamps timestamp

)

select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1 limit 100
drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1
(
Scenario  varchar(255),
fisical_YearMonth varchar(255),
Department varchar(255),
Team varchar(255),
"Expense level 1" varchar(255),
"Expense level 2" varchar(255),	
"Acc" varchar(255),	
"Accounts/Cost Elements" varchar(255),	
"Logic" varchar(255),	
"Index" varchar(255),	
"Comments" varchar(255),
Manual_Forecast_Adj decimal(25,10),
budget_index2 decimal(25,10),
Budget_ActualAvg_budget_index3 decimal(25,10),
"Budget_Actual_total_index5-6" decimal(25,10),
Budget_finalmonth_Actual_Index8 decimal(25,10),
Budget_ActualAvg_Salesv1_index9 decimal(25,10),
Budget_ActualAvg_Salesv1_index10 decimal(25,10),
Budget_Capexforecast_index7 decimal(25,10),
Budget_leasingforecast_index8 decimal(25,10),
Budget_eachmonth_index12 decimal(25,10),
budget_Allocations_index13 decimal(25,10),
Budget_Final decimal(25,10),
timestamps timestamp

)



select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Actual_HC;
drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Actual_HC;
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Actual_HC
(
F_YM varchar(255),
"Cost Center" varchar(255),
Name varchar(255),
Region varchar(255),
"Company Code" varchar(255),
"Profit Center" varchar(255),
"Person Responsible" varchar(255),
Category varchar(255),
Department varchar(255),
Team varchar(255),
Department_New varchar(255),
Team_new varchar(255),
Mark varchar(255),
quatity float,
timestamps timestamp

)

drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_CommitRnoForecast;
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_CommitRnoForecast
(
scenario varchar(255),
scenario_YM varchar(255),
fisical_YearMonth varchar(255),
"Team" varchar(255),
Des varchar(255),
"index" varchar(255),
start_fiscalYear varchar(255),
End_fiscalYear varchar(255),
"Expense level 1" varchar(255),
"Expense level 2" varchar(255),	
"Acc" varchar(255),	
"Accounts/Cost Elements" varchar(255),	
"Commit Logic base actual" varchar(255),	
"Commit Index base actual" varchar(255),	
"Commit Logic base forecast" varchar(255),	
"Commit Index base forecast" varchar(255),
last_actual_amount decimal(25,10),
Labor_Amount_baseActual decimal(25,10),
Labor_Amount_baseOutlook decimal(25,10),
YTD_Amount_CNY decimal(25,10),--new
YTD_Count_FW decimal(25,10),--new
Avg_Actual_YTD_Amount_CNY decimal(25,10),
Manual_Forecast_Adj decimal(25,10),
Capex_Forecast_budget decimal(25,10),
Lease_Forecast_budget decimal(25,10),
YTD_Actual_Sales decimal(25,10),
last_Actual_salesamount decimal(25,10),
forecast_sales_amount decimal(25,10),
forecast_sales_amount_YTD decimal(25,10),
Total_Budget_Final decimal(25,10),--new
outlook_Budget_Final decimal(25,10),
EngineeringAllocations_forecast_manualAdj decimal(25,10),
AvgOutlook_vs_Actual_baseonActual decimal(25,10), --new
AvgFW_budget_baseonActual decimal(25,10), --new
Des1 varchar(255),
Des2 varchar(255),
Budget_BaseonActual decimal(25,10),
Budget_Baseonoutlook decimal(25,10),
CommitRno_ForcastAmount decimal(25,10),
timestamps timestamp
)

select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ActualForecast_HC 
where Scenario='2025 AugRnO'
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ActualForecast_HC 
(
Scenario varchar(255),
F_YM varchar(255),
"Cost Center" varchar(255),
Name varchar(255),
Region varchar(255),
"Company Code" varchar(255),
"Profit Center" varchar(255),
"Person Responsible" varchar(255),
Category varchar(255),
Department varchar(255),
Team varchar(255),
Mark varchar(255),
quatity varchar(255),
timestamps timestamp
)


create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_LaborAllocation_BaseonActual
(
Scenario varchar(255),
F_YearMonth varchar(255),
Team varchar(255),
Acc varchar(255),
YTD_Actual_Amount_CNY decimal(25,10),
Total_YTD_Actual_Amount_CNY decimal(25,10),
YTD_Labor_Amount decimal(25,10),
ratio float,
Labor_Amount decimal(25,10),
timestamps timestamp
)

create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_LaborAllocation_BaseonForecast
(
Scenario varchar(255),
scenario_YM varchar(255),
F_YearMonth varchar(255),
Team varchar(255),
Acc varchar(255),
YTD_Budget_Amount_CNY decimal(25,10),
YTD_Labor_Amount decimal(25,10),
Total_YTD_Budget_Amount_CNY decimal(25,10),
ratio float,
Labor_Amount decimal(25,10),
timestamps timestamp
)