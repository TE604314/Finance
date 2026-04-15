CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_Plant_Losspn_ODS_to_DWD_fbl3n()
LANGUAGE plpgsql
AS $$
BEGIN

create temporary table FBL3N_Step1 as
select
   a."company code", 
   a.plant,
   a.material,
   a.account,
   a."Loss_Type",
   a."document number",
   a."document date",
   a."posting date",
   a."local currency",
   a."profit center",
   a."posting period",
   a.quantity,
   a."amount in local currency",
   b."Procurement type",
   c.inventory_movement_type_code,
   c.reason_for_movement_code,
   d.Reason_Name,
   c.local_currency_amount
from
(
select 
    "company code", 
	plant,
    material,
    account,
    case when account<>'5330050' then 1 else 0 end  as Movetypelink,
 	case when account in ('5340050','6060071','5009022') then 'scrap'
     when account in ('5330055') then 'normal loss' 
	 when account in ('5330050') then 'loss Tail difference adjustment'
     end as "Loss_Type",
    "document number",
    "document date",
     to_Date("posting date",'YYYY/MM/DD') as "posting date",
    "local currency",
    "profit center",
    "posting period",
    quantity,
	"amount in local currency"
from s3dataschema.ods_finance_plant_losspn_fbl3n 
where filename<>'sheetname' and quantity<>'' and quantity is not null and 
case when account in ('5330055','5330050') and quantity<0 then 0 else 1 end=1
) a

left join 

(

select plant_id,material_number,max(procurement_type_code) as "Procurement type"
from master_data.dimension_material_plants_current 
group by 
plant_id,
material_number
) b
on a.plant=b.plant_id and a.material=b.material_number

left join 

(
select 
source_plant_id,
source_te_corporate_part_number as material,
1 as Movetypelink, --5330050（不查PO 尾差调整）、5330055（正常盘亏）
to_date(inventory_movement_document_posting_date,'YYYY-MM-DD') as movement_document_posting_date,
base_unit_of_measure_quantitiy as measure_quantitiy,
max(case when inventory_movement_type_code in ('551','552','553','554','555','556') then 'scrap'
     when inventory_movement_type_code in ('711','712','717') then 'loss' end
) as "Loss_Type",
max(inventory_movement_type_code) as inventory_movement_type_code,
max(reason_for_movement_code) as reason_for_movement_code,
max(extended_true_cost_local_currency_amount_signed) as local_currency_amount
from inventory.inventory_movements
where inventory_movement_type_code in ('551','552','553','554','555','556','711','712','717') and
 to_char(to_DATE(inventory_movement_document_posting_date,'YYYY-MM-DD'),'YYYYMM')>'202509' and
company_code in ('1908','2554','3027','1641')
group by 
source_plant_id,
source_te_corporate_part_number,
inventory_movement_document_posting_date,
base_unit_of_measure_quantitiy
) c
on a.plant=c.source_plant_id and a.material=c.material and a."posting date"=c.movement_document_posting_date
and abs(TRUNC(a.quantity)) =abs(TRUNC(c.measure_quantitiy)) and a.Movetypelink=c.Movetypelink

left join 
(
select
    Reason_Code,
    max(Reason_Name) as Reason_Name
from s3dataschema.ods_finance_plant_losspn_ReasonName 
where Reason_Code<>'ReasonCode'
group by Reason_Code
) d
on cast(c.reason_for_movement_code as int)=d.Reason_Code;

delete ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_fbl3n;

insert into ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_fbl3n
(
 "company code", 
   plant,
   material,
   account,
   "Loss_Type",
   "document number",
   "document date",
   "posting date",
   "local currency",
   "profit center",
   "posting period",
    quantity,
   "amount in local currency",
   "Procurement type",
   inventory_movement_type_code,
   reason_for_movement_code,
   Reason_Name,
   local_currency_amount,
   timestamps
)
select
  "company code", 
   plant,
   material,
   account,
   "Loss_Type",
   "document number",
   "document date",
   "posting date",
   "local currency",
   "profit center",
   "posting period",
   cast(quantity as float) as quantity,
   cast("amount in local currency" as float) as "amount in local currency",
   "Procurement type",
   inventory_movement_type_code,
   reason_for_movement_code,
   Reason_Name,
   cast(local_currency_amount as float) as local_currency_amount,
   current_timestamp as timestamps
from FBL3N_Step1;


create temporary table Temp_PO as

select 
  a.plant,
  a.material,
  a."posting date",
  b.purchasing_document_id, --order
  b.purchasing_document_key_id,
  b.purchasing_document_item_id,
  coalesce(c.invoice_posting_date,d.invoice_posting_date,e.invoice_posting_date,f.invoice_posting_date) as invoice_posting_date, --开票时间
  --b.goods_receipt_posting_date, --入库时间
  b.original_item_creation_date, --创建时间
  b.vendor_key_id,
  b.vendor_id,
  b.vendor_name,
  coalesce(c.invoice_quantity,d.invoice_quantity,e.invoice_quantity,f.invoice_quantity) as invoice_quantity,--开票数量
  b.Order_Quantity,
  b.schedule_received_quantity,--入库数量
  b.received_amount,--入库金额
  coalesce(c.invoice_document_currency_amount,d.invoice_document_currency_amount,e.invoice_document_currency_amount,f.invoice_document_currency_amount) as invoice_document_currency_amount, --开票金额
  coalesce(c.invoice_flag,d.invoice_flag,e.invoice_flag,f.invoice_flag,'Null') as invoice_flag
from 
(
select
  distinct
   plant,
   material,
   "posting date"

from FBL3N_Step1
where "Procurement type" in ('F','X') and reason_for_movement_code not in ('0019','1021') AND account<>'5330050'
) a 

left join

(
select 
plant_id,
te_corporate_part_number,
purchasing_document_id, --order
purchasing_document_key_id,
purchasing_document_item_id,
--goods_receipt_posting_date, --入库时间 会有多比入库
max(original_item_creation_date) as original_item_creation_date, --创建时间
max(vendor_key_id) as vendor_key_id,
max(vendor_id) as vendor_id,
max(vendor_name) as vendor_name,
sum(schedule_quantity)  as Order_Quantity,
sum(schedule_received_quantity) as schedule_received_quantity,--入库数量
sum(document_currency_unit_price_amount*schedule_received_quantity)  as received_amount--入库金额
from purchasing.purchasing_document_combined
where coalesce(schedule_quantity,0)<>0
group by 
plant_id,
te_corporate_part_number,
purchasing_document_id, --order
purchasing_document_key_id,
purchasing_document_item_id
) b 
on a.plant=b.plant_id and a.material=b.te_corporate_part_number

left join
(
select
purchasing_document_id,
plant_id,
te_corporate_part_number,
max(invoice_posting_date) as invoice_posting_date, --开票时间
max(invoice_quantity) as invoice_quantity, --开票数量
max(invoice_document_currency_amount) as invoice_document_currency_amount, --开票金额
'invoice_documents_1' as invoice_flag
from purchasing.purchasing_invoice_documents
group by 
purchasing_document_id,
plant_id,
te_corporate_part_number
) c 
on a. plant=c.plant_id and a.material=c.te_corporate_part_number and b.purchasing_document_id=c.purchasing_document_id

--存在PN 为空的情况
left join

(
select
purchasing_document_id,
plant_id,
max(invoice_posting_date) as invoice_posting_date, --开票时间
max(invoice_quantity) as invoice_quantity, --开票数量
max(invoice_document_currency_amount) as invoice_document_currency_amount, --开票金额
'invoice_documents_2' as invoice_flag
from purchasing.purchasing_invoice_documents
group by 
purchasing_document_id,
plant_id
) d
on a. plant=d.plant_id and b.purchasing_document_id=d.purchasing_document_id

left join
(
select
purchasing_document_id,
max(invoice_posting_date) as invoice_posting_date, --开票时间
max(invoice_quantity) as invoice_quantity, --开票数量
max(invoice_document_currency_amount) as invoice_document_currency_amount, --开票金额
'invoice_documents_3' as invoice_flag
from purchasing.purchasing_invoice_documents
group by 
purchasing_document_id
) e
on  b.purchasing_document_id=e.purchasing_document_id

left join
(
select 
source_plant_id AS plant_id,
source_te_corporate_part_number as material,
purchase_order_id,
--purchase_order_item_id,
max(to_date(inventory_movement_document_posting_date,'YYYY-MM-DD')) as invoice_posting_date,
max(base_unit_of_measure_quantitiy) as invoice_quantity,
--base_unit_of_measure_quantity_signed,
--entry_unit_of_measure_quantity,
--entry_unit_of_measure_quantity_signed,
max(abs(extended_true_cost_local_currency_amount_signed)) as invoice_document_currency_amount,
'inventory_movements' as invoice_flag
from inventory.inventory_movements
where inventory_movement_type_code in ('101') and
company_code in ('1908','2554','3027','1641') 
group by 
source_plant_id,
source_te_corporate_part_number,
purchase_order_id
) f
on a. plant=f.plant_id and a.material=f.material and b.purchasing_document_id=f.purchase_order_id;

delete ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_PO;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_PO
(
  plant,
  material,
  "posting date",
  purchasing_document_id, --order
  purchasing_document_key_id,
  purchasing_document_item_id,
  invoice_posting_date, --开票时间
  original_item_creation_date, --创建时间
  vendor_key_id,
  vendor_id,
  vendor_name,
  invoice_quantity,--开票数量
  Order_Quantity,--订单数量
  schedule_received_quantity,--入库数量
  received_amount,--入库金额
  invoice_document_currency_amount, --开票金额
  invoice_flag,
  timestamps
)
select
 plant,
  material,
  "posting date",
  purchasing_document_id, --order
  purchasing_document_key_id,
  purchasing_document_item_id,
  invoice_posting_date, --开票时间
  original_item_creation_date, --创建时间
  vendor_key_id,
  vendor_id,
  vendor_name,
  invoice_quantity,--开票数量
  Order_Quantity,
  schedule_received_quantity,--入库数量
  received_amount,--入库金额
  invoice_document_currency_amount, --开票金额
  invoice_flag,
  current_timestamp as timestamps
from Temp_PO
where coalesce(received_amount,0)<>0 or invoice_flag<>'Null';

delete ss_rs_ts_aut_ap_digital_schema.DIM_finance_plant_losspn_Calendar;
insert into ss_rs_ts_aut_ap_digital_schema.DIM_finance_plant_losspn_Calendar
(
Year,
Yearmonth,
calendar_date,
timestamps
)
select 
left(calendar_date,4) as Year,
to_char(calendar_date,'YYYYMM') AS Yearmonth,
calendar_date,
current_timestamp as timestamps
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date" a
where to_char(calendar_date,'YYYYMM') between
(
select min(to_char(to_date("posting date",'YYYY-MM'),'YYYYMM')) FROM ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_PO
)
and 
(
select max(to_char(to_date("posting date",'YYYY-MM'),'YYYYMM')) FROM ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_PO
) ;






END;
$$;

select * from ss_rs_ts_aut_ap_digital_schema.Test
where source_plant_id='1303' 
select 
source_plant_id,
source_te_corporate_part_number as material,
purchase_order_id,
purchase_order_item_id,
to_date(inventory_movement_document_posting_date,'YYYY-MM-DD') as movement_document_posting_date,
inventory_movement_file_received_date,
base_unit_of_measure_quantitiy,
base_unit_of_measure_quantity_signed,
entry_unit_of_measure_quantity,
entry_unit_of_measure_quantity_signed,
extended_true_cost_local_currency_amount_signed as local_currency_amount
into ss_rs_ts_aut_ap_digital_schema.Test
from inventory.inventory_movements
where inventory_movement_type_code in ('101') and
company_code in ('1908','2554','3027','1641') and source_te_corporate_part_number='2136733-4'
and purchase_order_id='2733676091'

select count(*) from inventory.inventory_movements
where inventory_movement_type_code in ('101') and
company_code in ('1908','2554','3027','1641')


drop table ss_rs_ts_aut_ap_digital_schema.Test


select * from ss_rs_ts_aut_ap_digital_schema.Test
from purchasing.purchasing_document_combined
where coalesce(schedule_quantity,0)<>0

select *
into ss_rs_ts_aut_ap_digital_schema.Test
from purchasing.purchasing_document_combined
where coalesce(schedule_quantity,0)<>0 and plant_id='0916' AND
te_corporate_part_number='2322877-1'

select distinct purchasing_document_item_id,schedule_item_sequence_number_id 
from ss_rs_ts_aut_ap_digital_schema.Test

select 
plant_id,
te_corporate_part_number,
purchasing_document_id, --order
purchasing_document_key_id,
schedule_item_sequence_number_id,
--goods_receipt_posting_date, --入库时间 会有多比入库
original_item_creation_date, --创建时间
vendor_key_id,
vendor_id,
vendor_name,
document_currency_unit_price_amount,
schedule_quantity as Order_Quantity,
schedule_received_quantity,--入库数量
document_currency_unit_price_amount*schedule_received_quantity as received_amount--入库金额
into ss_rs_ts_aut_ap_digital_schema.Test
from purchasing.purchasing_document_combined
where coalesce(schedule_quantity,0)<>0 and purchasing_document_id='2710245950'

"Material number"
select
Level,
"Parent material",
"BOM item number",
ROW_NUMBER over(partition by level,"Parent material","BOM item number")
from  Export_PN_List
where "BOM item number"=10;

