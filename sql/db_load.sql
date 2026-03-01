SET client_encoding TO 'UTF8';

-- Group 1: no FK dependencies
\copy vendors FROM 'D:/Repos/distributor-dataset-generator/output/vendors.csv' CSV HEADER
\copy distribution_centers FROM 'D:/Repos/distributor-dataset-generator/output/distribution_centers.csv' CSV HEADER
\copy customers FROM 'D:/Repos/distributor-dataset-generator/output/customers.csv' CSV HEADER
\copy carriers FROM 'D:/Repos/distributor-dataset-generator/output/carriers.csv' CSV HEADER

-- Group 2: depend on Group 1
\copy items FROM 'D:/Repos/distributor-dataset-generator/output/items.csv' CSV HEADER
\copy vendor_performance FROM 'D:/Repos/distributor-dataset-generator/output/vendor_performance.csv' CSV HEADER
\copy sales_reps FROM 'D:/Repos/distributor-dataset-generator/output/sales_reps.csv' CSV HEADER
\copy fuel_costs FROM 'D:/Repos/distributor-dataset-generator/output/fuel_costs.csv' CSV HEADER

-- Group 3: depend on items
\copy item_costs FROM 'D:/Repos/distributor-dataset-generator/output/item_costs.csv' CSV HEADER
\copy vendor_item_details FROM 'D:/Repos/distributor-dataset-generator/output/vendor_item_details.csv' CSV HEADER
\copy item_forecast FROM 'D:/Repos/distributor-dataset-generator/output/item_forecast.csv' CSV HEADER
\copy inventory_monthly_snapshot FROM 'D:/Repos/distributor-dataset-generator/output/inventory_monthly_snapshot.csv' CSV HEADER
\copy inventory_adjustments FROM 'D:/Repos/distributor-dataset-generator/output/inventory_adjustments.csv' CSV HEADER
\copy inventory FROM 'D:/Repos/distributor-dataset-generator/output/inventory.csv' CSV HEADER
\copy dc_item_slotting FROM 'D:/Repos/distributor-dataset-generator/output/dc_item_slotting.csv' CSV HEADER
\copy customer_item_preferences FROM 'D:/Repos/distributor-dataset-generator/output/customer_item_preferences.csv' CSV HEADER
\copy price_history FROM 'D:/Repos/distributor-dataset-generator/output/price_history.csv' CSV HEADER
\copy purchase_orders FROM 'D:/Repos/distributor-dataset-generator/output/purchase_orders.csv' CSV HEADER

-- Group 4: orders and fulfillment (strict sequence)
\copy orders FROM 'D:/Repos/distributor-dataset-generator/output/orders.csv' CSV HEADER
\copy order_lines FROM 'D:/Repos/distributor-dataset-generator/output/order_lines.csv' CSV HEADER
\copy shipments FROM 'D:/Repos/distributor-dataset-generator/output/shipments.csv' CSV HEADER
\copy backorders FROM 'D:/Repos/distributor-dataset-generator/output/backorders.csv' CSV HEADER
\copy shipment_lines FROM 'D:/Repos/distributor-dataset-generator/output/shipment_lines.csv' CSV HEADER
\copy tracking_events FROM 'D:/Repos/distributor-dataset-generator/output/tracking_events.csv' CSV HEADER

-- Group 5: finance
\copy invoices FROM 'D:/Repos/distributor-dataset-generator/output/invoices.csv' CSV HEADER
\copy ar_ledger FROM 'D:/Repos/distributor-dataset-generator/output/ar_ledger.csv' CSV HEADER

-- Group 6: carrier costs
\copy driver_costs FROM 'D:/Repos/distributor-dataset-generator/output/driver_costs.csv' CSV HEADER

-- Group 7: extension tables
\copy customer_contacts FROM 'D:/Repos/distributor-dataset-generator/output/customer_contacts.csv' CSV HEADER
\copy promotions FROM 'D:/Repos/distributor-dataset-generator/output/promotions.csv' CSV HEADER
\copy promo_orders FROM 'D:/Repos/distributor-dataset-generator/output/promo_orders.csv' CSV HEADER
\copy returns FROM 'D:/Repos/distributor-dataset-generator/output/returns.csv' CSV HEADER
\copy labor_costs FROM 'D:/Repos/distributor-dataset-generator/output/labor_costs.csv' CSV HEADER
