select * from ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_KS13
where fiscal_year_week_id=202401
drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_KS13
CREATE TABLE ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_KS13
(
Fyear varchar(255),
"Cost Center"  varchar(255),
"Cost Center Category"  varchar(255),
Department  varchar(255),
"Company Code"  varchar(255),	
"Profit Center"  varchar(255),
"Country Key"  varchar(255),
City  varchar(255),
timesatamps timestamp
)

create table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_CostCenterInfo
(
cost_center_effective_from_date date,
cost_center_effective_to_date date,
cost_center_company_code varchar(255),
cost_center_department_number varchar(255),
cost_center_building_country_code varchar(255),
trimmed_cost_center_id varchar(255),
cost_center_profit_center_id varchar(255),
cost_center_category_code varchar(255),
cost_center_name varchar(255),
timesatamps varchar(255)
)

drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12Rawdata
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12Rawdata
(
Fyear varchar(255),
Account varchar(255),
Description varchar(255),
AccountDescription varchar(255),
CC_Flag varchar(255),
CostCenter varchar(255),
CostCenter2 varchar(255), 
Rowno INT,
"Per 1" decimal(28,10),
"Per 2" decimal(28,10),
"Per 3" decimal(28,10),
"Per 4" decimal(28,10),
"Per 5" decimal(28,10),
"Per 6" decimal(28,10),
"Per 7" decimal(28,10),
"Per 8" decimal(28,10),
"Per 9" decimal(28,10),
"Per 10" decimal(28,10),
"Per 11" decimal(28,10),
"Per 12" decimal(28,10),
timestamps timestamp
)

drop table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12
create table ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12
(
year varchar(255),
YearMonth varchar(255),
"location" varchar(255),
CostCenter2 varchar(255),
cost_center_category_code varchar(255), 
"Department" varchar(255), 
"Team" varchar(255), 
Account varchar(255),
Description varchar(255),
AccountDescription varchar(255),
Months varchar(255),
cny_usd_rate decimal(28,10),
twd_usd_rate decimal(28,10),
Original_Amount decimal(28,10),
Amount_CNY decimal(28,10),
timestamps timestamp
)

drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_Calendar
create table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_Calendar
(
Parent_Scenario varchar(255),
Child_Scenario varchar(255),
fiscal_year_id varchar(255),
fiscal_quarter_id varchar(255),
fiscal_quarter_code varchar(255),
fiscal_year_month_id varchar(255),
calendar_date varchar(255),
"Index" varchar(255),
timestamps timestamp
)

drop table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_ScenarioforUI
create table ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_ScenarioforUI
(
Title varchar(255),
present_Scenario varchar(255),
Scenario_A_link varchar(255),
Scenario_B_link varchar(255),
Ranks int
)