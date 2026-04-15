CREATE EXTERNAL TABLE s3dataschema.ods_finance_plant_losspn_fbl3n (
    filepath varchar(255),
    filename varchar(255),
    "company code" varchar(255),
    account varchar(255),
    document number varchar(255),
    plant varchar(255),
    assignment varchar(255),
    document date varchar(255),
    posting date varchar(255),
    document type varchar(255),
    posting key varchar(255),
    "amount in local currency" varchar(255),
    "local currency" varchar(255),
    "amount in doc. curr" varchar(255),
    "document currency" varchar(255),
    "profit center" varchar(255),
    text varchar(255),
    reversed with varchar(255),
    customer varchar(255),
    net due date varchar(255),
    clearing date varchar(255),
    arrears after net due date varchar(255),
    reference varchar(255),
    material varchar(255),
    quantity varchar(255),
    posting period varchar(255),
    item varchar(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/Plant/Loss PN List Report/FBL3N SAP/';


select * from s3dataschema.ods_finance_plant_losspn_ReasonName
CREATE EXTERNAL TABLE s3dataschema.ods_finance_plant_losspn_ReasonName (
    filepath varchar(255),
    filename varchar(255),
    Link_Key varchar(255),
    Reason_Code varchar(255),
    Reason_Name varchar(255)
    )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://te-aws-ss-prod-ts-aut-apac-digital-conformance-data/Apac_Digital/Finance_Department/FinanceCSV/Plant/Loss PN List Report/Mapping/ReasonName/';


drop table ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_fbl3n
create table ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_fbl3n
(
  "company code" varchar(255), 
   plant varchar(255),
   material varchar(255),
   account varchar(255),
   "Loss_Type" varchar(255),
   "document number" varchar(255),
   "document date" varchar(255),
   "posting date" varchar(255),
   "local currency" varchar(255),
   "profit center" varchar(255),
   "posting period" varchar(255),
   quantity float ,
   "amount in local currency" decimal(25,10),
   "Procurement type" varchar(255),
   inventory_movement_type_code varchar(255),
   reason_for_movement_code varchar(255),
   Reason_Name  varchar(255),
   local_currency_amount decimal(25,10),
   timestamps timestamp
)

drop table ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_PO
create table ss_rs_ts_aut_ap_digital_schema.DWD_finance_plant_losspn_PO
(
  plant varchar(255),
  material varchar(255),
  "posting date" varchar(255),
  purchasing_document_id varchar(255), --order
  purchasing_document_key_id varchar(255),
  purchasing_document_item_id varchar(255),
  invoice_posting_date varchar(255), --开票时间
  original_item_creation_date varchar(255), --创建时间
  vendor_key_id varchar(255),
  vendor_id varchar(255),
  vendor_name varchar(255),
  invoice_quantity FLOAT,--开票数量
  Order_Quantity FLOAT,
  schedule_received_quantity FLOAT,--入库数量
  received_amount decimal(25,10),--入库金额
  invoice_document_currency_amount decimal(25,10),--开票金额
   invoice_flag varchar(255),
   timestamps timestamp
)

select * from ss_rs_ts_aut_ap_digital_schema.DIM_finance_plant_losspn_Calendar
drop table ss_rs_ts_aut_ap_digital_schema.DIM_finance_plant_losspn_Calendar
create table ss_rs_ts_aut_ap_digital_schema.DIM_finance_plant_losspn_Calendar
(
   year varchar(255),
   Yearmonth varchar(255),
   calendar_date date,
   timestamps timestamp
)

