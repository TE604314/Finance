
select * from s3dataschema.ods_Finance_FPA_Opex_ZB12  limit 100
where Filename='ZB12_FY15.xlsx' limit 100

drop table s3dataschema.ods_Finance_FPA_Opex_ZB12
select * from s3dataschema.ods_Finance_FPA_Opex_ZB12
where filename='ZB12_FY25.xlsx' limit 100
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_ZB12
(
"Filename" varchar(255),
"sheetname" varchar(255),
AccountDescription varchar(255),
"Per 1" varchar(255),
"Per 2" varchar(255),
"Per 3" varchar(255),
"Per 4" varchar(255),
"Per 5" varchar(255),
"Per 6" varchar(255),
"Per 7" varchar(255),
"Per 8" varchar(255),
"Per 9" varchar(255),
"Per 10" varchar(255),
"Per 11" varchar(255),
"Per 12" varchar(255)


)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/ZB12/';

drop table s3dataschema.ods_Finance_FPA_Opex_KS13
select * from s3dataschema.ods_Finance_FPA_Opex_KS13 limit 100
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_KS13
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Cost Center" varchar(255),	
"Name" varchar(255),
"Cost Center Category" varchar(255),
"Department" varchar(255),
"Building Identifier" varchar(255),	
"Business Function Code" varchar(255),
"Currency" varchar(255),
"Company Code" varchar(255),	
"Profit Center" varchar(255),	
"Valid From" varchar(255),
"Valid To" varchar(255),	
"Actual: Primary Costs (Lock Indicator)" varchar(255),
"Person Responsible" varchar(255),
"User Responsible" varchar(255),
"Country Key" varchar(255),	
"City" varchar(255),
"Business Area" varchar(255),
"Controlling Area" varchar(255),
"Corporate Functional Org" varchar(255),
"Cost ctr short text" varchar(255),
"Costing Sheet" varchar(255),
"Created by" varchar(255),
"Created on" varchar(255),
"Description" varchar(255),
"Functional Area" varchar(255),
"Hierarchy Area" varchar(255),
"Object number" varchar(255),
"Overhead key" varchar(255),
"Recovery Indicator" varchar(255),
"Region" varchar(255),
"Title" varchar(255),
"Usage" varchar(255),
"Actual: Revenues (Lock Indicator)" varchar(255),
"Actual: Secondary Costs (Lock Indicator)" varchar(255)



)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/KS13/';

select * from s3dataschema.ods_Finance_FPA_Opex_CompanyInfo
drop table s3dataschema.ods_Finance_FPA_Opex_CompanyInfo
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_CompanyInfo
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Company code" varchar(255),
"location" varchar(255),
"Currency" varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Mapping/Manual Input/Company info/';


drop table s3dataschema.ods_Finance_FPA_Opex_ProfitCenter
select * from s3dataschema.ods_Finance_FPA_Opex_ProfitCenter
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_ProfitCenter
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Profit center" varchar(255),
"BU" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Mapping/Manual Input/PC/';

select * from s3dataschema.ods_Finance_FPA_Opex_CostElement

drop table s3dataschema.ods_Finance_FPA_Opex_CostElement
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_CostElement
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Cost element" varchar(255),
"Expense level 1" varchar(255),
"Expense level 2" varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Mapping/Manual Input/Cost Element/';



drop table s3dataschema.ods_Finance_FPA_Opex_FuctionArea
select * from s3dataschema.ods_Finance_FPA_Opex_FuctionArea
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_FuctionArea
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Function area" varchar(255),
"Department" varchar(255),
"Team" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Mapping/Manual Input/Fuction Area/';


drop table s3dataschema.ods_Finance_FPA_Opex_AccountExpCategory
select * from s3dataschema.ods_Finance_FPA_Opex_AccountExpCategory
where "Cost element" ='6081312'
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_AccountExpCategory
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Cost element" varchar(255),
"Expense level 1" varchar(255),	
"Expense level 2" varchar(255)


)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Mapping/Account-Exp Category/';


drop table s3dataschema.ods_Finance_FPA_Opex_CapexForecast
select * from s3dataschema.ods_Finance_FPA_Opex_CapexForecast limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_CapexForecast
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Index" varchar(255),
"Acc" varchar(255),
"C"	 varchar(255),
"D"	 varchar(255),
"E"	 varchar(255),
"F" varchar(255),
"Department" varchar(255),
"H" varchar(255),
"I" varchar(255),
"J" varchar(255),
"K" varchar(255),
"L" varchar(255),
"M" varchar(255),
"N" varchar(255),
"O" varchar(255),
"P" varchar(255),
"Q" varchar(255),
"R" varchar(255),
"S" varchar(255),
"T" varchar(255),
"U" varchar(255),
"V" varchar(255),
"W" varchar(255),
"X" varchar(255),
"Y" varchar(255),
P1	 varchar(255),
P2	 varchar(255),
P3 varchar(255),
P4 varchar(255),
P5 varchar(255),
P6 varchar(255),
P7 varchar(255),
P8 varchar(255),
P9 	 varchar(255),
P10 varchar(255),
P11 varchar(255),
P12 varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Forecast/CAPEX&Lease FCST/Capex Forecast/';


drop table s3dataschema.ods_Finance_FPA_Opex_LeaseForecast
select * from s3dataschema.ods_Finance_FPA_Opex_LeaseForecast limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_LeaseForecast
(
"Filename" varchar(255),
"sheetname" varchar(255),
"index"  varchar(255),
"Acc"  varchar(255),
"Description"  varchar(255),
"Business Unit"  varchar(255),
"Cost Center"  varchar(255),
"Department"  varchar(255),
"Start FYFM"  varchar(255),
"End FYFM"  varchar(255),
"period"  varchar(255),
"Total Leasing Amount"  varchar(255),
P1	 varchar(255),
P2	 varchar(255),
P3 varchar(255),
P4 varchar(255),
P5 varchar(255),
P6 varchar(255),
P7 varchar(255),
P8 varchar(255),
P9 	 varchar(255),
P10 varchar(255),
P11 varchar(255),
P12 varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Forecast/CAPEX&Lease FCST/Lease Forecast/';



drop table s3dataschema.ods_Finance_FPA_Opex_EngineeringAllocations
select * from s3dataschema.ods_Finance_FPA_Opex_EngineeringAllocations limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_EngineeringAllocations
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Index" varchar(255),
Department varchar(255),
"Expense level 1" varchar(255),
"Expense level 2" varchar(255),	
Acc	 varchar(255),	
"Accounts/Cost Elements" varchar(255),
P1	 varchar(255),
P2	 varchar(255),
P3 varchar(255),
P4 varchar(255),
P5 varchar(255),
P6 varchar(255),
P7 varchar(255),
P8 varchar(255),
P9 	 varchar(255),
P10 varchar(255),
P11 varchar(255),
P12 varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Forecast/Engineering Allocations/';



drop table s3dataschema.ods_Finance_FPA_Opex_ForecastLogic
select * from s3dataschema.ods_Finance_FPA_Opex_ForecastLogic
where acc='6060205'  limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_ForecastLogic
(
"Filename" varchar(255),
"sheetname" varchar(255),
start_fiscalYear varchar(255),
End_fiscalYear varchar(255),
"Expense level 1" varchar(255),
"Expense level 2" varchar(255),	
"Acc" varchar(255),	
"Accounts/Cost Elements" varchar(255),	
"Logic" varchar(255),	
"Index" varchar(255),	
"Comments" varchar(255),	
"Comments_vw" varchar(255),
"Commit Logic base actual" varchar(255),	
"Commit Index base actual" varchar(255),	
"Comments base actual" varchar(255),
"Commit Logic base forecast" varchar(255),	
"Commit Index base forecast" varchar(255),	
"Comments base forecast" varchar(255)


)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Forecast/Forecast Logic/';

show  table s3dataschema.ods_Finance_FPA_Opex_Opexexpensebudget
drop table s3dataschema.ods_Finance_FPA_Opex_Opexexpensebudget
select * from s3dataschema.ods_Finance_FPA_Opex_Opexexpensebudget limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_Opexexpensebudget
(
"Filename" varchar(255),
"sheetname" varchar(255),
"index" varchar(255),
department varchar(255),
"GL Acc" varchar(255),
Oct varchar(255),
Nov varchar(255),
"Dec"	 varchar(255),
Jan	 varchar(255),
Feb varchar(255),
Mar varchar(255),
Apr varchar(255),
May  varchar(255),
Jun varchar(255),
Jul varchar(255),
Aug varchar(255),
Sep varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Forecast/Opex expense budget/';



drop table s3dataschema.ods_Finance_FPA_Opex_Salesbudget
select * from s3dataschema.ods_Finance_FPA_Opex_Salesbudget limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_Salesbudget
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Fyear" varchar(255),
"Sales" varchar(255),
"YOY Increase%" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Forecast/Sales budget/';


drop table s3dataschema.ods_Finance_FPA_Opex_TempAUTExpense
select * from s3dataschema.ods_Finance_FPA_Opex_TempAUTExpense limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_TempAUTExpense
(
"Filename" varchar(255),
"sheetname" varchar(255),
Department varchar(255),
Team varchar(255),
"Team(Index)" varchar(255),
"Account" varchar(255),	
"Index" varchar(255),	
"NA H. 3 Name" varchar(255), 	
"NA H. 4 Name" varchar(255), 	
"Accounts/Cost Elements" varchar(255),	 
FY25P01 	  varchar(255),	 
FY25P02 	 varchar(255),	 
FY25P03 	 varchar(255),	 
FY25P04 	 varchar(255),	 
FY25P05 	  varchar(255),	 
FY25P06  varchar(255),	 
FY25P07 	 varchar(255),	 
FY25P08 	 varchar(255),	 
FY25P09 	 varchar(255),	 
FY25P10  varchar(255),	 
FY25P11  varchar(255),	 
FY25P12  varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Temp/AUT ESG&A Expense/';

select * from s3dataschema.Dim_Bom limit 100
drop table s3dataschema.Dim_Bom
CREATE external TABLE s3dataschema.Dim_Bom
(
Scenario varchar(255),
Companycode varchar(255),
"SFG material" varchar(255),
YM varchar(255),
SFG_Plant varchar(255),
FG_Plant varchar(255),
"FG material" varchar(255),
OrigGrp varchar(255),
BOM_flag varchar(255),
BaseUoM varchar(255),
FG_CostCenter varchar(255),
SFG_Quantity float,
FG_Sample_flag varchar(255),
FG_Labor_hours_perunit float,
FG_Tooling_sccop_perunit float,
FG_Labor_sccop_perunit float,
FG_Machine_hours_perunit float,
FG_Machine_sccop_perunit float,
FG_Sccope_standardcost_1time_perunit float,
FG_Sccope_standardcost_perunit float,
SFG_CostCenter varchar(255),
SFG_Sample_flag varchar(255),
SFG_Tooling_sccop_perunit float,
SFG_Labor_sccop_perunit float,
SFG_Labor_hours_perunit float,
SFG_Machine_hours_perunit float,
SFG_Machine_sccop_perunit float,
SFG_Sccope_standardcost_1time_perunit float,
SFG_Sccope_standardcost_perunit  float
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/Plant/Output&Sccop Report/BOM_Standcost/';



drop table s3dataschema.ods_Finance_FPA_Opex_KE5Z
select * from s3dataschema.ods_Finance_FPA_Opex_KE5Z limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_KE5Z
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Receiver CoCde" varchar(255),	
"Ref. Document" varchar(255),	
"Fiscal Year" varchar(255),	
"Posting period" varchar(255),	
"Profit Center" varchar(255),	
"Account Number" varchar(255),	
"In company code currency" varchar(255),	
"Entry Date" varchar(255),	
"Posting Date" varchar(255),	
"Quantity" varchar(255),	
"Cost Center" varchar(255),	
"Functional Area" varchar(255),	
"WBS Element" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/KE5Z/';


drop table s3dataschema.ods_Finance_FPA_Opex_projectlist
select * from s3dataschema.ods_Finance_FPA_Opex_projectlist limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_projectlist
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Project Number" varchar(255),
"Project Name" varchar(255),
"Project Status" varchar(255),
"PAC" varchar(255),
"Region" varchar(255),
"Type" varchar(255),
"Platform" varchar(255),
"ProductLine" varchar(255),
"Plant" varchar(255),
"Risk_Level" varchar(255),
"SalesTeam" varchar(255)




)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Mapping/Manual Input/ProjectList/';



drop table s3dataschema.ods_Finance_FPA_Opex_ForecastHeadCount
select * from s3dataschema.ods_Finance_FPA_Opex_ForecastHeadCount limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_ForecastHeadCount
(
"Filename" varchar(255),
"sheetname" varchar(255),
Team	varchar(255),
P1	varchar(255),
P2	varchar(255),
P3	varchar(255),
P4	varchar(255),
P5	varchar(255),
P6	varchar(255),
P7	varchar(255),
P8	varchar(255),
P9	varchar(255),
P10	varchar(255),
P11	varchar(255),
P12 varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Forecast/HeadCount Plan/HeadCount Plan/';




drop table s3dataschema.ods_Finance_FPA_Opex_Sales
select fisical_YM,Amount from s3dataschema.ods_Finance_FPA_Opex_Sales
where fisical_YM<>'Fisical' limit 100
where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_Sales
(
"Filename" varchar(255),
"sheetname" varchar(255),
fisical_YM varchar(255),
Amount varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Sales/Sales/';


select * from s3dataschema.ods_Finance_FPA_Opex_ScenarioMap

where 
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_ScenarioMap
(
"Filename" varchar(255),
"sheetname" varchar(255),
scenario varchar(255),
Des varchar(255),
"index" varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Mapping/Scenario Map/Scenario/';

select * from s3dataschema.ods_Finance_FPA_Opex_parameter
drop table s3dataschema.ods_Finance_FPA_Opex_parameter
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_parameter
(
"Filename" varchar(255),
"sheetname" varchar(255),
Fyear varchar(255),
Des varchar(255),
Des2 varchar(255)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Mapping/Scenario Map/parameter/';


select * from s3dataschema.ods_Finance_FPA_Opex_ActualHC
drop table s3dataschema.ods_Finance_FPA_Opex_ActualHC
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_ActualHC
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Cost Center" varchar(255),
Name	varchar(255),
Region	 varchar(255),
"Company Code" varchar(255),
"Profit Center" varchar(255),
"Person Responsible" varchar(255),
Category	 varchar(255),
Department	 varchar(255),
Team	varchar(255),
Mark	varchar(255),
"HC-P1" varchar(255),
"HC-P2" varchar(255),
"HC-P3" varchar(255),
"HC-P4" varchar(255),
"HC-P5" varchar(255),
"HC-P6" varchar(255),
"HC-P7" varchar(255),
"HC-P8" varchar(255),
"HC-P9" varchar(255),
"HC-P10" varchar(255),
"HC-P11" varchar(255),
"HC-P12" varchar(255)


)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Actual/HeadCount/';


select * from s3dataschema.ods_Finance_FPA_Opex_ForecastManualAdj
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_ForecastManualAdj
(
"Filename" varchar(255),
"sheetname" varchar(255),
Team  varchar(255),
Acc  varchar(255),
P1  varchar(255),
P2  varchar(255),
P3  varchar(255),
P4  varchar(255),
P5  varchar(255),
P6  varchar(255),
P7  varchar(255),
P8  varchar(255),
P9  varchar(255),
P10  varchar(255),
P11  varchar(255),
P12  varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Forecast/Manual Adj/';



select * from s3dataschema.ods_Finance_FPA_Opex_ForecastLaborAdj
CREATE external TABLE s3dataschema.ods_Finance_FPA_Opex_ForecastLaborAdj
(
"Filename" varchar(255),
"sheetname" varchar(255),
"Index" varchar(255),
"Team" varchar(255),
"Expense level 1" varchar(255),
"Expense level 2" varchar(255),
"Acc"	 varchar(255),
"Bonus"	 varchar(255),
"Accounts/Cost Elements" varchar(255),
"P1"	 varchar(255),
"P2"	varchar(255),
"P3"	varchar(255),
"P4"	varchar(255),
"P5"	varchar(255),
"P6"	varchar(255),
"P7"	varchar(255),
"P8"	varchar(255),
"P9"	varchar(255),
"P10"	varchar(255),
"P11"	varchar(255),
"P12" varchar(255)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/FPnA Digital Project/Opex digital project/Forecast/Labor cost adjustment/';



