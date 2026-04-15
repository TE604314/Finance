call ss_rs_ts_aut_ap_digital_schema.sp_Finance_Controlling_PRDelivery_ODS_to_DWD_PR();

CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_Controlling_PRDelivery_ODS_to_DWD_PR()
LANGUAGE plpgsql
AS $$
begin

create temporary table Temp_PR AS
select 
purchase_order_release_date,
original_request_date,
requisition_number_id,
purchasing_document_id,
vendor_part_number,
delete_blocked_indicator_code,
currency_code,
company_code,
plant_id,
vendor_id,
vendor_name,
purchase_order_quantity,
document_currency_net_price_amount,
functional_currency_unit_price_amount
from purchasing.purchasing_document_combined
where company_code in ('1908','2554','3027','1641') and 
TO_CHAR(purchase_order_release_date, 'YYYYMM')>=202602 and UPPER(coalesce(delete_blocked_indicator_code,'Null'))<>'L';

delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_Controlling_PRDelivery_PRFact;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_Controlling_PRDelivery_PRFact
(
purchase_order_release_date,
original_request_date,
requisition_number_id,
purchasing_document_id,
vendor_part_number,
delete_blocked_indicator_code,
currency_code,
company_code,
plant_id,
vendor_id,
vendor_name,
purchase_order_quantity,
document_currency_net_price_amount,
functional_currency_unit_price_amount,
counts,
timestamps
)
select
a.purchase_order_release_date,
a.original_request_date,
a.requisition_number_id,
a.purchasing_document_id,
a.vendor_part_number,
a.delete_blocked_indicator_code,
a.currency_code,
a.company_code,
a.plant_id,
a.vendor_id,
a.vendor_name,
a.purchase_order_quantity,
a.document_currency_net_price_amount,
a.functional_currency_unit_price_amount,
b.counts,
CURRENT_TIMESTAMP as timestamps
from
(
select * from Temp_PR
where  requisition_number_id is not null and 
length(cast(vendor_id as bigint))<>4 and vendor_id is not null and vendor_id<>'' and 
vendor_part_number not like '%#%' and length(vendor_part_number)<15
) a 

left join

(
select 
requisition_number_id,
plant_id,
vendor_part_number,
vendor_id,
purchase_order_release_date,
original_request_date,
count(distinct purchasing_document_id) as counts
from Temp_PR
where  requisition_number_id is not null and 
length(cast(vendor_id as bigint))<>4 and vendor_id is not null and vendor_id<>'' and 
vendor_part_number not like '%#%' and length(vendor_part_number)<15
group by requisition_number_id,
plant_id,
vendor_part_number,
vendor_id,
original_request_date,
purchase_order_release_date
having count(distinct purchasing_document_id)>1
) b
on a.requisition_number_id=b.requisition_number_id and a.plant_id=b.plant_id and 
a.vendor_id=b.vendor_id 
and a.original_request_date=b.original_request_date and a.purchase_order_release_date=b.purchase_order_release_date
 and a.vendor_part_number=b.vendor_part_number;

DROP TABLE Temp_PR;

delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_Controlling_PRDelivery_IOBO;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_Controlling_PRDelivery_IOBO
(
Filename,
"Act. Gds Mvmnt Date",
"Approval Date",
 day_diff,
"Ship Before Approval Indicator",
"Approval Time",
"TED Profit Center",
"Approver name",
"Status",
"Delivery Document",
"Delivery Document item",
"Root cause",
"Freight Currency",
"Freight Amount",
"Net Price",
"PF Carrier",
"PF Carrier name",
"PF Special proc. indicator",
"Requestor name",
"Submission date",
"Sold To Name",	
"Sales Office",	
"Ship from Country",	
"Ship To Name",	
"Ship to Country",	
"Material",	
"Warehouse",	
"Product hierarchy",	
"Product line",	
"Quantity",	
"Total Weight",	
"Sales Unit",	
"Profit Center",	
"Ship-to party",	
"Sold-to Party",	
"MRP Controller",	
"Sales Document",	
"Sales Document item",	
"Country of origin",	
"Sales Group",	
"Purchase Document",	
"Purchase Document item",	
"Customer Material",	
"Sales Organization",	
"Shipment Number",	
"External ID",	
"Delivery Date",	
"Plant Code",	
"Plant Name",	
"Premium Freight Flag",	
"Workitem ID",	
"Cust No at Carrier",	
"Freight Bill-to",
timestamps
)
select 
Filepath AS Filename,
"Act. Gds Mvmnt Date",
"Approval Date",
--DATEDIFF(day,TO_DATE("Approval Date",'YYYY/MM/DD'),TO_DATE("Act. Gds Mvmnt Date", 'YYYY/MM/DD')) AS day_diff,
DATEDIFF(
        day,
        to_date("Approval Date",'YYYY/MM/DD'),
        to_date("Act. Gds Mvmnt Date",'YYYY/MM/DD')
    ) AS day_diff,
"Ship Before Approval Indicator",
"Approval Time",
"TED Profit Center",
"Approver name",
"Status",
"Delivery Document",
"Delivery Document item",
"Root cause",
"Freight Currency",
cast("Freight Amount" as float) as "Freight Amount",
cast("Net Price" as float) as "Net Price",
"PF Carrier",
"PF Carrier name",
"PF Special proc. indicator",
"Requestor name",
"Submission date",
"Sold To Name",	
"Sales Office",	
"Ship from Country",	
"Ship To Name",	
"Ship to Country",	
"Material",	
"Warehouse",	
"Product hierarchy",	
"Product line",	
cast("Quantity" as float) as "Quantity",	
cast("Total Weight" as float) as "Total Weight",	
"Sales Unit",	
"Profit Center",	
"Ship-to party",	
"Sold-to Party",	
"MRP Controller",	
"Sales Document",	
"Sales Document item",	
"Country of origin",	
"Sales Group",	
"Purchase Document",	
"Purchase Document item",	
"Customer Material",	
"Sales Organization",	
"Shipment Number",	
"External ID",	
"Delivery Date",	
"Plant Code",	
"Plant Name",	
"Premium Freight Flag",	
"Workitem ID",	
"Cust No at Carrier",	
"Freight Bill-to",
current_timestamp as timestamps
from s3dataschema.ods_Finance_Controlling_PRDelivery_IOBO
where Filename<>'sheetname';

delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_Controlling_PRDelivery_SZPF;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_Controlling_PRDelivery_SZPF 
(
"Filename",
"Delivery Document",
"Shipment Number",
"Requestor name",	
"Submission date",
"Act. Gds Mvmnt Date",		
"Approval Date",	
day_diff,
"Approval Time",	
"Original Loading Date",
"Freight Amount",	
"Freight Currency",	
"PF Carrier name",	
"PF Carrier",	
"Status",	
"Comments(Line 1)",	
"Ship-to party",	
"Sold-to Party",	
"Sold To Name",	
"Sales Document",	
"Purchase Document",	
"Material",	
"Quantity",	
"Delivery Date",	
"Shipment Carrier Name",	
"Root cause",	
"Comments(Line 2)",	
"Comments(Line 3)",	
"Comments(Line 4)",	
"Cancellation Date",	
"Cancelled By",	
"Freight Bill-to",	
"Loading Date",	
"Location id",	
"TMS LOAD ID",	
"Original Delivery Date",
"Plant Code",
"Premium Freight Flag",
"Profit Center",
"Rejected Date",
"Sales Organization",
"Ship To Name",
"Total Weight",
"Vendor",
"Weight unit",
"Workitem ID",
"Route",
timestamps
)
select
Filepath AS "Filename",
"Delivery Document",
"Shipment Number",
"Requestor name",	
"Submission date",
"Act. Gds Mvmnt Date",		
"Approval Date",	
DATEDIFF(day, to_date("Approval Date",'YYYY/MM/DD'), TO_DATE("Act. Gds Mvmnt Date",'YYYY/MM/DD')) AS day_diff,
"Approval Time",	
"Original Loading Date",
cast("Freight Amount" as float) as "Freight Amount",	
"Freight Currency",	
"PF Carrier name",	
"PF Carrier",	
"Status",	
"Comments(Line 1)",	
"Ship-to party",	
"Sold-to Party",	
"Sold To Name",	
"Sales Document",	
"Purchase Document",	
"Material",	
cast("Quantity" as float) as Quantity,	
"Delivery Date",	
"Shipment Carrier Name",	
"Root cause",	
"Comments(Line 2)",	
"Comments(Line 3)",	
"Comments(Line 4)",	
"Cancellation Date",	
"Cancelled By",	
"Freight Bill-to",	
"Loading Date",	
"Location id",	
"TMS LOAD ID",	
"Original Delivery Date",
"Plant Code",
"Premium Freight Flag",
"Profit Center",
"Rejected Date",
"Sales Organization",
"Ship To Name",
cast("Total Weight" as float) as "Total Weight",
"Vendor",
"Weight unit",
"Workitem ID",
"Route",
current_timestamp as timestamps
from s3dataschema.ods_Finance_Controlling_PRDelivery_SZPF
where Filename<>'sheetname';


END;
$$;




