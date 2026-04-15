call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_ARBAD_ODS_to_DWD_BillingSales()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_ARBAD_ODS_to_DWD_BillingSales()
LANGUAGE plpgsql
AS $$
begin
	




delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_SalesBilling;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_ARBAD_SalesBilling
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
"Product Line",	
"Sourcing",	
"GPL",	
"WWC",
timestamps
)

select 
concat("Fiscal Year","Fiscal Week Short Number") as fiscal_year_week_id,
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
"Product Line",	
"Sourcing",	
"GPL",	
"WWC",
current_timestamp as timestamps
from s3dataschema.ods_Finance_FPA_ARBAD_Billing 
where filepath<>'filename';



END;
$$;