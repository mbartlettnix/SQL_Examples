DEFINE MACRO DateFrom 120;
-- DEFINE MACRO master_table     brbrbrbrb.masterFulfillmentData;

//Following Three Tables are to insure Date/Store for all Stores as long as they have incurred an order in the last 50 days.
//Will create many empty rows, in particular with stores that have been offboarded, but this is okay.
DEFINE INLINE TABLE AvailDate (
  SELECT
  Event_Date,
  num_days_ago_int
  FROM
  googlestuff.window_availability_store_aggr, (SELECT checkout_date as Event_date, num_days_ago_int FROM googlestuff.delivery_estimate_availability_store GROUP BY 1,2)
  WHERE num_days_ago_int <= $DateFrom AND num_days_ago_int >= 1
  GROUP BY 1,2
);
  
DEFINE INLINE TABLE AvailStore (
  SELECT
  Store,
  Fulfillment_Market
  FROM
  googlestuff.window_availability_store_aggr, 
  (SELECT store, fulfillment_market, num_days_ago_int FROM googlestuff.delivery_estimate_availability_store GROUP BY 1,2,3)
  WHERE num_days_ago_int <= $DateFrom AND num_days_ago_int >= 1
  AND Store != " Total"
  GROUP BY 1,2
);

//Date / Store Table
DEFINE INLINE TABLE AvailDateStore (
  SELECT
  DATE(AvailDate.Event_Date) AS Event_Date,
  AvailDate.num_days_ago_int as num_days_ago_int,
  AvailStore.Store AS Store,
  AvailStore.Fulfillment_Market as Fulfillment_Market
  FROM AvailDate
  CROSS JOIN AvailStore
);
----------------------------------- Metric Calls Below --------------------------
//Availability Table

DEFINE INLINE TABLE new_avail_table (
  SELECT 
    DATE(checkout_date) as checkout_date,
    store,
    num_days_ago_int,
    SUM(first_win_is_avai_impressions) as first_win_is_avai_impressions,
    SUM(second_win_is_avai_impressions) as second_win_is_avai_impressions,
    SUM(third_win_is_avai_impressions) as third_win_is_avai_impressions,
    SUM(total_impressions) as total_impressions
  FROM googlestuff.delivery_estimate_availability_store
  WHERE num_days_ago_int <= $DateFrom AND num_days_ago_int >= 1
  GROUP BY 1,2,3
);

//OIPD Table -- Volume by Pack Day CHANGED: 1/21/18 FROM gsx_reporting_dremel.FulfilledByStore
DEFINE INLINE TABLE OIPD(
  SELECT
       DATE(fulfilled_date) AS Pack_Day,
       storename AS Store,
       fulfillment_market as Market,
       moo_goo AS MOO,
       num_days_ago_int,
       SUM(item_cnt) AS Items,
       SUM(parcel_cnt) AS Parcels,
       UINT32(0) AS StoreOrders,
       UINT32(0) AS MerchantOrders
FROM googlestuff.fulfilled_volume_summary
WHERE aggr_type = 'Day' AND num_days_ago_int <= $DateFrom AND num_days_ago_int >= 1
GROUP BY 1,2,3,4,5,8,9
);

DEFINE INLINE TABLE VCPMO(
  SELECT 
       fulfillment_market AS market,
       StoreName as Store,
       SUM(items) as Items,
       SUM (storeOrders) as SO,
       SUM (merchant_orders) as MO,
       DATE (OriginalDelWeek) as Date
FROM googlestuff.vcpmo_volume_store
GROUP BY 1,2,6
);

//OIDD Table - Volume by Delivery Date for DPMO measurements
DEFINE INLINE TABLE OIDD 
SELECT DATE(delivered_date) AS Delivery_Date,
       //num_days_ago_int,
       StoreName AS Store,
       fulfillment_market AS Market,
       SUM(delivered_items) AS Items,
       SUM(delivered_parcels) AS Parcels,
       SUM(delivered_orders) AS StoreOrders,
       SUM(delivered_merchant_orders) AS MerchantOrders
FROM googlestuff.delivered_volume
WHERE num_days_ago_int <= $DateFrom AND num_days_ago_int >= 1
  GROUP BY
  Delivery_Date, num_days_ago_int, Market, Store;    

//PPO Table - On-Time Pack
DEFINE INLINE TABLE PPO (
  SELECT
  DATE(pack_cutoff_date) as Pack_Cutoff_Date,
  StoreName as Store,
  fulfillment_market as Market,
  SUM(tot_ontime_parcels) as Ontime_Parcels,
  SUM(tot_parcels) as Total_Parcels,  
  SUM(tot_ontime_parcels) / SUM(tot_parcels) as Ontime_Percentage
  FROM
  googlestuff.LatePackSO_cutoff
  WHERE
  num_days_ago_int <= $DateFrom AND num_days_ago_int >= 1
  GROUP BY
  Pack_Cutoff_Date, Store, Market
);


//Ops Dash Rejection Table
//Updated to this table as of 7/11/17 due to EOS being out of commission for 3 weeks straight
DEFINE INLINE TABLE OpsDashRej (
  SELECT 
  DATE(Process_Reject_Swap_Date) as Process_Date,
  StoreName,
  SUM(primary_filled_items) as OpsDash_primary_filled_items,
  SUM(processed_items) as OpsDash_processed_items,
  SUM(rejected_items) as OpsDash_rejected_items,
  //SUM(swapped_items) as OpsDash_swapped_items
  FROM googlestuff.workDriver_item_fulfilled_rejected
  WHERE num_days_ago_int <= $DateFrom AND num_days_ago_int >= 1
  GROUP BY 1,2
);

//HandOff Table
DEFINE INLINE TABLE Handoff ( 
  SELECT
  DATE(pack_cutoff_date) as Pack_Date,
  StoreName as Store,
  fulfillment_market as Market,
  SUM(tot_Parcels) as Handoff_Total_Parcels,
  SUM(Parcels_ontime_handoff) as Handoff_Parcels_Handed_Off_On_Time,
  SUM(Parcels_Late_handoff) as Handoff_Parcels_Handed_Off_Late,  
  SUM(Parcels_no_logged_handoff) as Handoff_Parcels_Not_Marked_Handedoff,
  SUM(parcels_no_handoff_window) AS Handoff_Parcels_No_Handoff_Window
  FROM
  googlestuff.ops.handoff_perf
  WHERE
  num_days_ago_int <= $DateFrom AND num_days_ago_int >= 1
  GROUP BY
  Pack_Date, Store, Market
);

// THIS SHOULD BE UPDATED WITH coming 1/1/18. UPDATE 1/21/18: WAITING ON SOLUTION
DEFINE INLINE TABLE Forecast (
  SELECT
  DATE(Date) as Forecast_Date,
  Store,
  Day_Of_Lock_Forecast,
  Day_Of_Lock_MO,  
  Two_Week_Lock_Forecast,
  Two_Week_Lock_MO,
  One_Week_Lock_Forecast,
  One_Week_Lock_MO
  FROM
  brbrbrbrb.AllMarketsForecasting  
  WHERE Date != 'null'
);

DEFINE INLINE TABLE StoreReference (
  SELECT
  StoreName as Store,
  MOO as MOO,
  merchant_id, 
  merchant_name,
  store_code   
  FROM
  googlestuff.StoreTable
);
//Must call Labor data via other scripts, Engine is different
// MAY BE DEPRECATED IN THE NEAR FUTURE TO FALCON AS SOURCE OF TRUTH
DEFINE INLINE TABLE Labor (
  SELECT
  DATE(Date) as Labor_Date,
  Store,
  Scheduled_Hours,
  Actual_Hours
  FROM
  brbrbrbrb.LaborHours
  WHERE Date != 'null'
);

--------------UPDATE DPMO SOURCE ----------------
DEFINE INLINE TABLE New_DPMO (
SELECT DATE(Date) AS Date,
       Market,
       Store,
       Defect_Count,
       Raw_Defect_Count,
       Damaged_Count,
       Incorrect_Count,
       Expired_Count,
       Missing_Count,
       Mispay_Count,
       Store_Package_Missing_Count
FROM googlestuff.DPMO_Metric
);

-------------- ADD FULFILLMENT SUCCESS ----------------
DEFINE INLINE TABLE Ful_Success (
SELECT 
  DATE(processed_date) as processed_date, 
  fulfillment_market,
  storeName, 
  SUM(filled_items) AS filled_items, 
  SUM(total_attempts) AS total_attemps,
  SUM(not_filled_attempts) AS not_filled_attemps
FROM googlestuff.fulfillment_success_rate
WHERE num_days_ago_int <= $DateFrom AND num_days_ago_int >= 1
GROUP BY 3,1,2
ORDER BY processed_date DESC
);

//ADDITION OF FALCON DATA
//Uses Reference to convert Merchant_ID + Store_Code into Ops Dash 2.0 Store Name
DEFINE INLINE TABLE Falcon ( 
  SELECT *
  FROM brbrbrbr.FalconByStore
  );

// This script that combines all key Metrics for the past 100 Days
// Author: brbrbrbrb

// Create table
-- CREATE OR REPLACE TABLE $master_table AS
  SELECT  
  AvailDateStore.event_date as Pack_Day,
  AvailDateStore.Fulfillment_Market as Market,  
  AvailDateStore.Store as Store,  
  AvailDateStore.num_days_ago_int,
  CASE WHEN StoreReference.MOO IS NULL THEN '' WHEN StoreReference.MOO = 'N/A' THEN 'Merchant Direct' ELSE StoreReference.MOO END as MOO,
  StoreReference.merchant_name as Merchant_Name,
  OIPD.Items,         // Items Processed
  CASE WHEN (OIPD.MOO = "MOO") THEN CAST(OIPD.Items*0 AS UINT64) ELSE OIPD.Items END AS nonMOO_Items,
  OIPD.Parcels,       //Parcels Processed
  OIPD.StoreOrders as StoreOrders,
  OIPD.MerchantOrders as MerchantOrders,
  CASE WHEN (OIPD.MOO = "MOO") THEN CAST(OIPD.MerchantOrders*0 AS UINT64) ELSE OIPD.MerchantOrders END AS nonMOO_MerchantOrders,
  VCPMO.Items,
  VCPMO.SO,
  VCPMO.MO,
  PPO.Ontime_Parcels as Ontime_Parcels,
  PPO.Total_Parcels as Total_Parcels,  
  Handoff.Handoff_Total_Parcels,
  Handoff.Handoff_Parcels_Handed_Off_On_Time,
  Handoff.Handoff_Parcels_Handed_Off_Late,  
  Handoff.Handoff_Parcels_Not_Marked_Handedoff,
  Forecast.Day_Of_Lock_Forecast as Day_Of_Lock_Forecast,
  Forecast.Day_Of_Lock_MO,
  Forecast.Two_Week_Lock_Forecast as Two_Week_Lock_Forecast,  
  Forecast.Two_Week_Lock_MO,
  Forecast.One_Week_Lock_Forecast as One_Week_Lock_Forecast,  
  Forecast.One_Week_Lock_MO,
  Labor.Scheduled_Hours,
  Labor.Actual_Hours,
  OIDD.Items as Items_Delivered,
  Falcon.Falcon_Pick_Count,
  Falcon.Falcon_Pay_Count,
  Falcon.Falcon_Pack_Count,
  Falcon.Falcon_Pick_Hours,
  Falcon.Falcon_Pay_Hours,
  Falcon.Falcon_Pack_Hours,
  Falcon.Falcon_Shift_Duration_Hours, 
  Falcon.Falcon_Paid_Hours, 
  Falcon.Falcon_Direct_Hours, 
  Falcon.Falcon_Indirect_Hours, 
  Falcon.Falcon_Lunch_Hours,
  Falcon.Falcon_Off_Task_Hours,
  Falcon.Falcon_Lack_of_Volume_Hours,
  Falcon.Falcon_Non_GSX_Hours,
  OpsDashRej.OpsDash_primary_filled_items,
  OpsDashRej.OpsDash_processed_items,
  OpsDashRej.OpsDash_rejected_items,
  t17.first_win_is_avai_impressions as new_first_win_is_avai_impressions,
  t17.second_win_is_avai_impressions as new_second_wind_is_avai_impressions,
  t17.third_win_is_avai_impressions as new_third_win_is_avai_impressions,
  t17.total_impressions as new_total_impressions,
  Ful_Success.filled_items as Ful_Success_Filled_Items,
  Ful_Success.total_attemps as Ful_Success_Attempted_Items,
  Ful_Success.not_filled_attemps AS Ful_Success_not_filled_attemps,
  New_DPMO.Defect_Count AS New_Defect_Count,
  New_DPMO.Raw_Defect_Count AS New_Raw_Defect_Count,
  New_DPMO.Damaged_Count AS New_Damaged_Count,
  New_DPMO.Incorrect_Count AS New_Incorrect_Count,
  New_DPMO.Expired_Count AS New_Expired_Count,
  New_DPMO.Missing_Count AS New_Missing_Count,
  New_DPMO.Mispay_Count AS New_Mispay_Count
  

  FROM AvailDateStore
  LEFT OUTER JOIN PPO ON (AvailDateStore.Event_Date = PPO.Pack_Cutoff_Date AND AvailDateStore.Store = PPO.Store)  
  LEFT OUTER JOIN OIPD ON (AvailDateStore.Event_Date = OIPD.pack_day AND AvailDateStore.Store = OIPD.Store)
  LEFT OUTER JOIN OIDD ON (AvailDateStore.Event_Date = OIDD.Delivery_date AND AvailDateStore.Store = OIDD.Store) 
  LEFT OUTER JOIN Handoff ON (AvailDateStore.Event_Date = Handoff.Pack_Date AND AvailDateStore.Store = Handoff.Store)
  LEFT OUTER JOIN Forecast ON (AvailDateStore.Event_Date = Forecast.Forecast_Date AND AvailDateStore.Store = Forecast.Store)
  LEFT OUTER JOIN StoreReference ON (AvailDateStore.Store = StoreReference.Store)
  LEFT OUTER JOIN Labor ON (AvailDateStore.Event_Date = Labor.Labor_Date AND AvailDateStore.Store = Labor.Store)
  LEFT OUTER JOIN Falcon ON (AvailDateStore.Event_Date = Falcon.Event_Date AND AvailDateStore.Store = Falcon.Store)
  LEFT OUTER JOIN OpsDashRej ON (AvailDateStore.Event_Date = OpsDashRej.Process_Date AND AvailDateStore.Store = OpsDashRej.StoreName)
  LEFT OUTER JOIN new_avail_table t17 ON AvailDateStore.Event_Date = t17.checkout_date AND AvailDateStore.Store = t17.store
  LEFT OUTER JOIN Ful_Success ON (AvailDateStore.Event_Date = Ful_Success.processed_date AND AvailDateStore.Store = Ful_Success.storeName)
  LEFT OUTER JOIN New_DPMO ON (AvailDateStore.Event_Date = New_DPMO.Date AND AvailDateStore.Store = New_DPMO.Store)
  LEFT OUTER JOIN VCPMO ON (AvailDateStore.Event_Date = VCPMO.Date AND AvailDateStore.Store = VCPMO.Store) 
 ORDER BY
  Pack_Day DESC, Store
;
// Grant permission to those in the <user-list> to read the table
-- GRANT READER ON TABLE $master_table TO 