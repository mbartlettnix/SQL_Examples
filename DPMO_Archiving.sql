----------------------------Reference Table for Merchant Naming ----------------------
DEFINE INLINE TABLE Reference
SELECT merchant_name,
       StoreName as store_name,
FROM gsx_reporting_dremel.StoreTable;

---------------------------- Box Recommendation Table Call ----------------------------
DEFINE INLINE TABLE DPMO_Box
  SELECT item_id,
       t2.parcel_id as parcel_id,
       t2.parcel_level_compliance,
       t2.eng_box_id,
       t2.actual_box_id,
       CASE 
        WHEN t2.parcel_level_compliance == "Compliant" THEN True
        ELSE False END AS Used_Rec
  FROM gsx_ful_quality.boxrec_condensed;
---------------------------- Trans Data for path tracking ----------------------------
DEFINE INLINE TABLE Trans_Data
SELECT service_level AS service_level,
       ParcelId AS Parcel_ID,
       num_days_ago_int
FROM gsx_reporting_dremel.OrigDelivPerf
WHERE num_days_ago_int <=15
;
--------------------------- GTIN & OP ID--------------------------- 
-- DEFINE INLINE TABLE GTIN
-- SELECT item_id,
--        scanned_barcode,
--        overriden_barcode,
--        CASE
--        WHEN overriden_barcode IS NULL THEN FALSE ELSE TRUE END AS GTIN_Overide,
-- FROM gsx_ful_quality.Gtin;
-- 
DEFINE INLINE TABLE ID_pick
SELECT operator_email,
        MAX(operator_id)AS operator_id
FROM gsx_bi.prod.operator_id_email_mapping
GROUP BY 1;
DEFINE INLINE TABLE ID_pay
SELECT operator_email,
        MAX(operator_id)AS operator_id
FROM gsx_bi.prod.operator_id_email_mapping
GROUP BY 1;
DEFINE INLINE TABLE ID_pack
SELECT operator_email,
        MAX(operator_id)AS operator_id
FROM gsx_bi.prod.operator_id_email_mapping
GROUP BY 1;
---------------------------- Call from scrub sheet ----------------------------
DEFINE INLINE TABLE Quality_Scrub
  SELECT * FROM gsx_ful_quality.DPMO_scrub
  WHERE PARSE_TIME_USEC(report_date) > DATE_ADD(NOW(),-14, "DAY") ; //change this during the transition
  
---------------------------- Combine all of the Inline Tables above ----------------------------
DEFINE MACRO DPMO_Scrub 
SELECT
--   CASE WHEN GTIN.overriden_barcode IS NULL THEN FALSE ELSE TRUE END AS GTIN_Overide,
  CASE WHEN Trans_Data.service_level IS NULL THEN "" ELSE Trans_Data.service_level END AS Facility_Count,
--   GTIN.scanned_barcode as GTIN,
  DPMO_Box.t2.eng_box_id as Recommended_Box_ID,
  DPMO_Box.t2.actual_box_id as Used_Box_ID,
  DPMO_Box.Used_Rec as Used_Rec,
  False AS GTIN_Overide,
  "" AS GTIN,
  Reference.merchant_name as Merchant_Name,
  Quality_Scrub.report_date as report_date,
  Quality_Scrub.fulfillment_zone as fulfillment_zone,
  Quality_Scrub.gvoc as gvoc,
  Quality_Scrub.locationcourier as locationcourier,
  Quality_Scrub.oms_order_id as oms_order_id,
  Quality_Scrub.gcase as gcase,
  DPMO_Box.parcel_id as Parcel_ID,
  Quality_Scrub.item_id as item_id,
  Quality_Scrub.picked_by as picked_by,
  Quality_Scrub.packed_by as packed_by,
  CASE WHEN Quality_Scrub.pay_email is null THEN "" ELSE Quality_Scrub.pay_email END as paid_by,
  Quality_Scrub.Mispay_Discrepancy as Mispay_Amount,
  CASE 
        WHEN Quality_Scrub.item_title is null THEN ""
        ELSE Quality_Scrub.item_title END AS item_title,
--   Quality_Scrub.item_title as item_title,
  Quality_Scrub.item_price as item_price,
  Quality_Scrub.interim_zone as interim_zone,
  Quality_Scrub.courier as courier,
  Quality_Scrub.division_responsible as division_responsible,
  Quality_Scrub.error_1 as error_1,
  Quality_Scrub.error_2 as error_2,
  Quality_Scrub.error_3 as error_3,
  Quality_Scrub.error_4 as error_4,
  Quality_Scrub.cs_accuracy as cs_accuracy,
  Quality_Scrub.operator_error as operator_error,
  Quality_Scrub.prod_ops_refund_reason as prod_ops_refund_reason,
  Quality_Scrub.comments as comments,
  Quality_Scrub.week as week,
--   0 as picked_by_id,
--   0 as paid_by_id,
--   0 as packed_by_id,
  CASE WHEN ID_pick.operator_email == Quality_Scrub.picked_by THEN INT64(ID_pick.operator_id) ELSE 0 END AS picked_by_id,
  CASE WHEN ID_pay.operator_email == Quality_Scrub.pay_email THEN INT64(ID_pay.operator_id) ELSE 0 END AS paid_by_id,
  CASE WHEN ID_pack.operator_email == Quality_Scrub.packed_by THEN INT64(ID_pack.operator_id) ELSE 0 END AS packed_by_id
FROM Quality_Scrub
LEFT OUTER JOIN Reference ON (Reference.store_name = Quality_Scrub.locationcourier)
LEFT OUTER JOIN DPMO_Box ON (DPMO_Box.item_id = Quality_Scrub.item_id)
LEFT OUTER JOIN Trans_Data ON (Trans_Data.Parcel_ID = DPMO_Box.parcel_id)
-- LEFT OUTER JOIN mbartlett.Gtin GTIN ON (GTIN.item_id = Quality_Scrub.item_id)
LEFT OUTER JOIN ID_pick ON (ID_pick.operator_email = Quality_Scrub.picked_by )
LEFT OUTER JOIN ID_pay ON (ID_pay.operator_email = Quality_Scrub.pay_email )
LEFT OUTER JOIN ID_pack ON (ID_pack.operator_email = Quality_Scrub.packed_by )
;
   
---------------------------- Call existing Archive that is more than 10 days ----------------------------
DEFINE MACRO DPMO_Archive
  SELECT * FROM gsx_ful_quality.DPMO_Archive
  WHERE PARSE_TIME_USEC(report_date) <= DATE_ADD(NOW(),-14, "DAY") ; // change line 54 if you are changing this

----------------------------Combine DPMO Scrub with existing archive

CREATE OR REPLACE TABLE googlestuff.DPMO_Archive AS 
SELECT 
  * 
  FROM (SELECT * FROM ($DPMO_Archive)),($DPMO_Scrub) 
  WHERE report_date != 'null'           
;
GRANT OWNER ON TABLE --------- 

-- SELECT COUNT(*)
-- FROM ($DPMO_Scrub);