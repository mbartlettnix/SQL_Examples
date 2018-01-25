DEFINE INLINE TABLE Forecast    
-- SELECT 
--   Date,
--   Fulfillment_Market,
--   SUM(Four_Week_Lock_Forecast) AS Forecast,
--   SUM(Four_Week_Lock_MO) AS ForecastMO,
-- FROM mbmbmbmb.FourWeekForecastArchive
-- GROUP BY 1,2;
SELECT Date,
       Fulfillment_Market,
       SUM(Four_Week_Lock_Forecast) AS Forecast,
       SUM(Four_Week_Lock_MO) AS ForecastMO,
FROM brbrbrbr.FourWeekForecastFulMarket
GROUP BY 1,2;


DEFINE INLINE TABLE OKR         SELECT * FROM mbmbmbmb.OKR_Table;

DEFINE INLINE TABLE Cost        SELECT * FROM mbmbmbmb.fulfillmentCost;
                                
DEFINE INLINE TABLE Request     SELECT budget_request_for_the_following_weeks_weekend,
                                       SUM(what_is_the_change_amount) AS what_is_the_change_amount ,
                                       market FROM mbmbmbmb.BudgetRequest
                                       GROUP BY 1,3;
// UPDATE TO googlestuff.StoreTable
DEFINE INLINE TABLE Reference    SELECT territory,region,fulfillment_market, FROM mbmbmbmb.Reference GROUP BY 3,2,1 ;

DEFINE INLINE TABLE Metric     
SELECT 
  CAST(Pack_Day AS STRING) AS Pack_Day, 
  Monthofyear(Cast(Pack_Day AS STRING)) AS MonthNum,
  Market, 
  SUM(Items) AS Items, 
  SUM(nonMOO_Items) AS nonMOO_Items, 
  SUM(MerchantOrders) AS MerchantOrders, 
  SUM(nonMOO_MerchantOrders) AS nonMOO_MerchantOrders, 
  SUM(Ontime_Parcels) AS Ontime_Parcels, 
  SUM(Total_Parcels) AS Total_Parcels, 
  SUM(New_Defect_Count) AS dpmo_scrubbed_items_impacted,
  SUM(Items_Delivered) AS Items_Delivered, 
  SUM(OpsDash_Primary_filled_items) AS OpsDash_Primary_filled_items, 
  SUM(OpsDash_Processed_Items) AS OpsDash_Processed_Items, 
  SUM(OpsDash_rejected_Items) AS OpsDash_rejected_Items,
  SUM(num_days_ago_int) AS num_days_ago_int,
  SUM(new_first_win_is_avai_impressions) AS new_first_win_is_avai_impressions,
  SUM(new_second_wind_is_avai_impressions) AS new_second_wind_is_avai_impressions,
  SUM(new_third_win_is_avai_impressions) AS new_third_win_is_avai_impressions, 
  SUM(new_total_impressions) AS new_total_impressions,
  SUM(New_Defect_Count) AS New_Defect_Count
  
FROM brbrbrbr.masterFulfillmentData
WHERE Market IS NOT NULL
GROUP BY 1,2,3;
 ----------------------------------------------- ----------------------------------------------- ----------------------------------------------- -----------------------------------------------

//create new series in Metrics for Costco MOO and GOO volume. 


//Combine ALL 
CREATE OR REPLACE TABLE mbmbmbmb.CentralBudgetTable AS
SELECT  
  Forecast.Forecast AS Forecast,
  Forecast.ForecastMO AS ForecastMO,
  -----------------------------------------------
  OKR.vcpmo AS VCPMO_OKR,
  OKR.vcpi AS VCPI_OKR,
--   OKR.on_time_pack AS OTP_OKR,
--   OKR.first_sla AS FirstSLA_OKR,
--   OKR.third_sla AS ThirdSLA_OKR,
--   OKR.dpmo AS DPMO_OKR,
--   OKR.fill_rate AS FR_OKR,
--   OKR.itemsmo AS ItemsMO_OKR,
--   OKR.costco_moo AS CostcoMOO_OKR,
--   OKR.costco_goo AS CostcoGOO_OKR,
--   OKR.non_costco_moo AS NonCostcoMOO_OKR,
--   OKR.non_costco_goo AS NonCostcoGOO_OKR,
  OKR.instore_labor AS InstoreLabor_OKR,
  OKR.costco_chargeback AS CostcoCharge_OKR,
  OKR.staging AS Staging_OKR,
  OKR.quality__fulfillment AS QualityCost_OKR,
  OKR.packaging AS PackagingCost_OKR,
  -----------------------------------------------
  Cost.instore_labor,
  Cost.costco_chargeback,
  //Cost.leads_local,
  COALESCE(Cost.staging, '0') as Cost.staging,
  Cost.quality__fulfillment,
  Cost.moo_funding,
  //Cost._3plfield_ops,
  //Cost.other_store_support,
  Cost.box__mailer,
  Cost.dunnage,
  Cost.other,
  Cost.shipping,
  -----------------------------------------------
  Request.what_is_the_change_amount AS RequestAmount,
  //Request.what_is_the_driver_for_the_change AS RequestDriver,
  //Request.plan_of_action AS RequestPlan,
   -----------------------------------------------
  Reference.territory AS Territory,
  Reference.region AS Region,
   -----------------------------------------------
  Metric.Pack_Day AS Date, 
  Metric.Market AS Market, 
  Metric.Items AS Items, 
  Metric.nonMOO_Items AS NonMOOItems, 
  Metric.MerchantOrders AS MO, 
  Metric.nonMOO_MerchantOrders AS NonMOO_MO, 
  Metric.Ontime_Parcels AS Ontime_Parcels, 
  Metric.Total_Parcels AS Total_Parcels, 
  Metric.dpmo_scrubbed_items_impacted AS DPMO_Items_Impacted, 
  Metric.Items_Delivered AS DPMO_Items_Delivered, 
  Metric.OpsDash_Processed_Items AS Processed_Items, 
  Metric.OpsDash_rejected_Items AS Rejected_Items,
  Metric.new_first_win_is_avai_impressions AS FirstAvailImpressions,
  Metric.new_second_wind_is_avai_impressions AS SecondAvailImpression,
  Metric.new_third_win_is_avai_impressions AS ThirdAvailImpressions, 
  Metric.new_total_impressions AS TotalImpressions,
  Metric.New_Defect_Count AS New_Defect_Count

  FROM Metric
  LEFT OUTER JOIN Forecast ON (Metric.Pack_Day = Forecast.Date AND Metric.Market = Forecast.Fulfillment_Market)
  LEFT OUTER JOIN OKR ON (Metric.MonthNum = OKR.month_number AND Metric.Market = OKR.Area_OKR) 
  LEFT OUTER JOIN Cost ON (Metric.Pack_Day = Cost.reporting_week AND Metric.Market = Cost.Market)
  LEFT OUTER JOIN Request ON (Metric.Pack_Day = Request.budget_request_for_the_following_weeks_weekend AND Metric.Market = Request.market)
  LEFT OUTER JOIN Reference ON (Metric.Market = Reference.fulfillment_market)
  ;
  
GRANT OWNER ON TABLE