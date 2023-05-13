--Source: https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis


--Rename dataset to 'DataCoDataset


--Check for Duplicates

with DuplicateCheck as (SELECT *,
Row_Number() OVER (
Partition by [benefit per order], [Customer ID], [Customer Lname], [Customer Street], [order date (DateOrders)], [Order Customer ID],
[Order Item Discount Rate], [Order Item Total], [Shipping Date (DateOrders)]
ORDER BY [Customer ID]) row_number
FROM DataCoDataset
)
SELECT *
FROM DuplicateCheck 
Where Row_number>1


--Change [order date (DateOrders)] and [shipping date (DateOrders)] columns to YYYY-MM-DD hh:mm:ss format

Alter Table DataCoDataset
Add [OrderDate] smalldatetime

Update DataCoDataset
SET [OrderDate] = convert(smalldatetime, [order date (DateOrders)])

Alter Table DataCoDataset
DROP COLUMN [order date (DateOrders)]

Alter Table DataCoDataset
Add [ShippingDate] smalldatetime

Update DataCoDataset
SET [ShippingDate] = convert(smalldatetime, [Shipping Date (DateOrders)])

Alter Table DataCoDataset
DROP COLUMN [shipping date (DateOrders)]


--Remove unneccessary columns

Alter Table DataCoDataset
DROP COLUMN [Customer Email], [Customer Fname], [Customer Lname], [Customer Password], [Customer Street], [Customer Zipcode], [Latitude], [Longitude], 
[Order Zipcode], [Product Description], [Product Image], [Benefit per Order], [Sales per customer], [Late_Delivery_risk], [Category ID], [Product Price], 
[Order Customer ID], [Product Status], [Department Id], [Product Card Id], [Product Category Id]


--Create new column: [Order Cost]

Alter Table DataCoDataset
Add [Order Cost] decimal(10,2)

Update DataCoDataset
SET [Order Cost] = [Order Item Total] - [Order Profit Per Order]


--Create new column: [orderstatus - simplified]

Alter Table DataCoDataset
Add [orderstatus - simplified] varchar(50)

Update DataCoDataset
SET [orderstatus - simplified] = CASE 
WHEN [Order Status] = 'complete' then 'Complete or Expecting'
WHEN [Order Status] = 'pending' then 'Complete or Expecting'
WHEN [Order Status] = 'closed' then 'Incomplete'
WHEN [Order Status] = 'payment_review' then 'Complete or Expecting'
WHEN [Order Status] = 'suspected_fraud' then 'Incomplete'
WHEN [Order Status] = 'on_hold' then 'Complete or Expecting'
WHEN [Order Status] = 'processing' then 'Complete or Expecting'
WHEN [Order Status] = 'canceled' then 'Incomplete'
WHEN [Order Status] = 'pending_payment' then 'Complete or Expecting'
END

--Rename columns

EXEC sp_rename 'dbo.DataCoDataset.type', 'Payment_Type', 'COLUMN'

EXEC sp_rename 'dbo.DataCoDataset.Days for shipping (real)', 'Shipment Days (Actual)', 'COLUMN'

EXEC sp_rename 'dbo.DataCoDataset.Days for shipment (scheduled)', 'Shipment Days (Scheduled)', 'COLUMN'

EXEC sp_rename 'dbo.DataCoDataset.Category Name', 'Product Category', 'COLUMN'

EXEC sp_rename 'dbo.DataCoDataset.Department Name', 'Product Department', 'COLUMN'

EXEC sp_rename 'dbo.DataCoDataset.Market', 'Seller Market', 'COLUMN'

EXEC sp_rename 'dbo.DataCoDataset.Order Item Cardprod ID', 'Product ID', 'COLUMN'

EXEC sp_rename 'dbo.DataCoDataset.Sales', 'Order Item Sales', 'COLUMN'


--Rename country values

Update DataCoDataset
SET [Customer Country] = 'United States'
WHERE [Customer Country] = 'EE. UU.'

Update DataCoDataset
SET [Order Country] = 'United States'
WHERE [Order Country] = 'Estados Unidos'

Update DataCoDataset
SET [Order Country] = 'United Kingdom'
WHERE [Order Country] = 'reino unido'


--View revised dataset

SELECT * FROM DataCoDataset


--Create view for orders

Create view [DataCo Orders] 
AS
Select [Order Id], [OrderDate], [Customer Id], [Delivery Status], 
sum([Order Item Sales]) [Order Sales], sum([Order Item Discount]) [Order Discounts], 
sum([Order Item Total]) [Order Total], sum([Order Cost]) [Order Costs], sum([Order Profit Per Order]) [Order Profit],
[Customer City], [Customer State], [Customer Country], [Customer Segment], 
[Seller Market], [Order City], [Order State], [Order Country], [Order Region], 
[Payment_Type], [Shipment Days (Actual)], [Shipment Days (Scheduled)], [ShippingDate]
FROM DataCoDataset
WHERE [orderstatus - simplified] = 'complete or expecting'
GROUP BY [Order Id], [OrderDate], [Customer Id], [Delivery Status], [Customer City], [Customer State], [Customer Country], [Customer Segment], 
[Seller Market], [Order City], [Order State], [Order Country], [Order Region], [Payment_Type], [Shipment Days (Actual)], [Shipment Days (Scheduled)], 
[ShippingDate]


--Create view to determine profitability of customer when fulfilling 1st and 2nd unprofitable orders

Create view [Customer Profits]
as
with [Customer Profits] as
(Select [Customer ID], sum([Order Profit]) [Customer Profit],
CASE WHEN sum([Order Profit]) >=0 THEN 'Profitable' ELSE 'Unprofitable' END AS [Customer Orders - Profit Category]
FROM [DataCo Orders]
GROUP BY [Customer ID]
)
,
[Unprofitable Orders Rank] as 
(
SELECT [Customer ID], [OrderDate], Row_number() OVER (Partition by [Customer ID] Order by [OrderDate] ASC) [Negative Order Rank]
FROM [DataCo Orders]
WHERE [Order Profit] < 0
)
,
[Earliest Unprofitable Orders] as
(
Select [Customer ID], CASE WHEN [Negative Order Rank] = '1' THEN [OrderDate] END [1st Negative Order], 
CASE WHEN [Negative Order Rank] = '2' THEN [OrderDate] END [2nd Negative Order]
FROM [Unprofitable Orders Rank]
)
,
[Orders Starting at 1st Negative Order] AS
(
SELECT o1.[Customer ID], o1.[Order ID], o1.[OrderDate], o1.[Order Profit], 
sum(o1.[Order Profit]) OVER (Partition by o1.[Customer ID]) [Customer Profit Starting at 1st Unprofitable Order],
Row_Number() OVER (Partition by o1.[Customer ID] Order by [OrderDate] ASC) [1st Order Rank]
FROM [DataCo Orders] o1
LEFT JOIN [Earliest Unprofitable Orders] o2
ON o1.[Customer ID]=o2.[Customer ID]
WHERE o1.[OrderDate] >= o2.[1st Negative Order]
)
,
[Orders Before 1st Negative Order] AS
(
SELECT o1.[Customer ID], o1.[Order ID], o1.[OrderDate], o1.[Order Profit],
sum(o1.[Order Profit]) OVER (Partition by o1.[Customer ID]) [Customer Profit Before 1st Unprofitable Order]
FROM [DataCo Orders] o1
LEFT JOIN [Earliest Unprofitable Orders] o2
ON o1.[Customer ID]=o2.[Customer ID]
WHERE o1.[OrderDate] < o2.[1st Negative Order]
)
,
[Customer Profits Before and Starting at 1st Negative Order] AS 
(
Select s1.[Customer ID], avg([Customer Profit Starting at 1st Unprofitable Order]) [Profit Starting at 1st Negative Order], 
avg([Customer Profit Before 1st Unprofitable Order]) [Profit Before 1st Negative Order]
FROM [Orders Starting at 1st Negative Order]  s1
FULL OUTER JOIN [Orders Before 1st Negative Order] s2
ON s1.[Customer ID] = s2.[Customer ID]
GROUP BY s1.[Customer ID]
)
,
[Orders Starting at 2nd Negative Order] AS
(
SELECT o1.[Customer ID], o1.[Order ID], o1.[OrderDate], o1.[Order Profit],
sum(o1.[Order Profit]) OVER (Partition by o1.[Customer iD]) [Customer Profit Starting at 2nd Unprofitable Order]
FROM [DataCo Orders] o1
LEFT JOIN [Earliest Unprofitable Orders] o2
ON o1.[Customer ID]=o2.[Customer ID]
WHERE o1.[OrderDate] >= o2.[2nd Negative Order]
)
,
[Orders Before 2nd Negative Order] AS
(
SELECT o1.[Customer ID], o1.[Order ID], o1.[OrderDate], o1.[Order Profit],
sum(o1.[Order Profit]) OVER (Partition by o1.[Customer iD]) [Customer Profit Before 2nd Unprofitable Order]
FROM [DataCo Orders] o1
LEFT JOIN [Earliest Unprofitable Orders] o2
ON o1.[Customer ID]=o2.[Customer ID]
WHERE o1.[OrderDate] < o2.[2nd Negative Order]
)
,
[Customer Profits Before and Starting at 2nd Negative Order] AS
(
Select s1.[Customer ID], avg([Customer Profit Starting at 2nd Unprofitable Order]) [Profit Starting at 2nd Negative Order], 
avg([Customer Profit Before 2nd Unprofitable Order]) [Profit Before 2nd Negative Order]
FROM [Orders Starting at 2nd Negative Order]  s1
FULL OUTER JOIN [Orders Before 2nd Negative Order] s2
ON s1.[Customer ID] = s2.[Customer ID]
GROUP BY s1.[Customer ID]
)
,
[Customer Profits 1st and 2nd Negative Orders] AS
(
SELECT p1.[Customer ID], p1.[Profit Before 1st Negative Order], p1.[Profit Starting at 1st Negative Order],
p2.[Profit Before 2nd Negative Order], p2.[Profit Starting at 2nd Negative Order]
FROM [Customer Profits Before and Starting at 1st Negative Order] p1
LEFT JOIN [Customer Profits Before and Starting at 2nd Negative Order] p2
ON p1.[Customer ID]=p2.[Customer iD]
)
,
[Categories] as
(
Select t1.[Customer ID], [Customer Profit], [Customer Orders - Profit Category],
Cast ([Profit Before 1st Negative Order] AS Decimal(10,2)) [Profit Before 1st Negative Order],
CASE WHEN [Profit Before 1st Negative Order] > 0 THEN 'Profitable' 
	 WHEN [Profit Before 1st Negative Order] < 0 THEN 'Unprofitable'  
	 WHEN [Profit Before 1st Negative Order] is NULL and [Profit Starting at 1st Negative Order] is NOT NULL THEN 'First Order Negative'
	 WHEN [Profit Before 1st Negative Order] is NULL and [Profit Starting at 1st Negative Order] is NULL THEN 'All Profitable Orders' 
	 END AS [Orders before 1st Negative Order - Profit Category],
Cast([Profit Starting at 1st Negative Order] AS Decimal(10,2)) [Profit Starting at 1st Negative Order], 
CASE WHEN [Profit Starting at 1st Negative Order] > 0 THEN 'Profitable' 
	 WHEN [Profit Starting at 1st Negative Order] < 0 THEN 'Unprofitable' ELSE 'All Profitable Orders' 
	 END AS [Orders starting at 1st Negative Order - Profit Category],
Cast ([Profit Before 2nd Negative Order] AS Decimal(10,2)) [Profit Before 2nd Negative Order],
CASE WHEN [Profit Before 2nd Negative Order] > 0 THEN 'Profitable' 
	 WHEN [Profit Before 2nd Negative Order] < 0 THEN 'Unprofitable' 
	 WHEN [Profit Before 2nd Negative Order] is NULL THEN 'No 2nd Unprofitable Order'
	 END AS [Orders before 2nd Negative Order - Profit Category],
Cast ([Profit Starting at 2nd Negative Order] AS Decimal(10,2)) [Profit Starting at 2nd Negative Order],
CASE WHEN [Profit Starting at 2nd Negative Order] > 0 THEN 'Profitable' 
	 WHEN [Profit Starting at 2nd Negative Order] < 0 THEN 'Unprofitable' 
	 WHEN [Profit Starting at 2nd Negative Order] is NULL THEN 'No 2nd Unprofitable Order'
	 END AS [Orders starting at 2nd Negative Order - Profit Category]
FROM [Customer Profits] t1
LEFT JOIN [Customer Profits 1st and 2nd Negative Orders] t2
ON t1.[Customer ID]=t2.[Customer ID]
ORDER BY [Customer ID] ASC
)
SELECT [Customer ID], [Customer Profit], [Customer Orders - Profit Category],
[Profit Before 1st Negative Order], [Orders before 1st Negative Order - Profit Category],
[Profit Starting at 1st Negative Order], [Orders starting at 1st Negative Order - Profit Category], 
CASE WHEN [Customer Orders - Profit Category] = 'Unprofitable' AND [Orders before 1st Negative Order - Profit Category] = 'Profitable' AND
[Orders starting at 1st Negative Order - Profit Category] = 'Unprofitable' THEN 'Customer turned Unprofitable through 1st Negative Order' 
	WHEN [Orders before 1st Negative Order - Profit Category] = 'First Order Negative' AND 
	[Orders starting at 1st Negative Order - Profit Category] = 'Unprofitable'
	THEN 'Customer acquired but Unprofitable through 1st Negative Order'
	WHEN [Orders before 1st Negative Order - Profit Category] = 'First Order Negative' AND
	[Orders starting at 1st Negative Order - Profit Category] = 'Profitable'
	THEN 'Customer acquired and Profitable through 1st Negative Order'
END as [Effect of 1st Negative Order],
[Profit Before 2nd Negative Order], [Orders before 2nd Negative Order - Profit Category],
[Profit Starting at 2nd Negative Order], [Orders starting at 2nd Negative Order - Profit Category], 
CASE WHEN [Customer Orders - Profit Category] = 'Unprofitable' AND [Orders before 2nd Negative Order - Profit Category] = 'Profitable' AND
[Orders starting at 2nd Negative Order - Profit Category] = 'Unprofitable' THEN 'Customer turned Unprofitable through 2nd Negative Order'
	 WHEN [Customer Orders - Profit Category] = 'Profitable' AND [Orders before 2nd Negative Order - Profit Category] = 'Unprofitable' AND
[Orders starting at 2nd Negative Order - Profit Category] = 'Profitable' THEN 'Customer turned Profitable through 2nd Negative Order'
END AS [Effect of 2nd Negative Order]
FROM [Categories]


--Create view for the orders after the 1st unprofitable order

Create view [Orders after 1st Unprofitable Order]
AS
with [Unprofitable Orders Rank] as
(SELECT [Customer ID], [OrderDate], Row_number() OVER (Partition by [Customer ID] Order by [OrderDate] ASC) [Negative Order Rank]
FROM [DataCo Orders]
WHERE [Order Profit] < 0
)
,
[Earliest Unprofitable Orders] as
(
Select [Customer ID], CASE WHEN [Negative Order Rank] = '1' THEN [OrderDate] END [1st Negative Order], 
CASE WHEN [Negative Order Rank] = '2' THEN [OrderDate] END [2nd Negative Order]
FROM [Unprofitable Orders Rank]
)
,
[Orders Starting at 1st Negative Order] AS
(
SELECT o1.[Customer ID], o1.[Order ID], o1.[OrderDate], o1.[Order Profit], 
sum(o1.[Order Profit]) OVER (Partition by o1.[Customer ID]) [Customer Profit Starting at 1st Unprofitable Order],
Row_Number() OVER (Partition by o1.[Customer ID] Order by [OrderDate] ASC) [1st Order Rank]
FROM [DataCo Orders] o1
LEFT JOIN [Earliest Unprofitable Orders] o2
ON o1.[Customer ID]=o2.[Customer ID]
WHERE o1.[OrderDate] >= o2.[1st Negative Order]
)
SELECT [Customer ID], [Order ID], [OrderDate], [Order Profit], [Customer Profit Starting at 1st Unprofitable Order], [1st Order Rank],
CASE WHEN [1st Order Rank] = 1 THEN '1st Unprofitable Order'
	 WHEN [1st Order Rank] = 2 THEN '1st Order After'
	 WHEN [1st Order Rank] = 3 THEN '2nd Order After'
	 WHEN [1st Order Rank] = 4 THEN '3nd Order After'
	 WHEN [1st Order Rank] = 5 THEN '4nd Order After'
	 WHEN [1st Order Rank] = 6 THEN '5th Order After'
	 END AS [1st Order Ranking]
FROM [Orders Starting at 1st Negative Order]
WHERE [1st Order Rank] <= 6


--Create view for the orders after the 2nd unprofitable order

Create view [Orders after 2nd Unprofitable Order]
AS
with [Unprofitable Orders Rank] as
(SELECT [Customer ID], [OrderDate], Row_number() OVER (Partition by [Customer ID] Order by [OrderDate] ASC) [Negative Order Rank]
FROM [DataCo Orders]
WHERE [Order Profit] < 0
)
,
[Earliest Unprofitable Orders] as
(
Select [Customer ID], CASE WHEN [Negative Order Rank] = '1' THEN [OrderDate] END [1st Negative Order], 
CASE WHEN [Negative Order Rank] = '2' THEN [OrderDate] END [2nd Negative Order]
FROM [Unprofitable Orders Rank]
)
,
[Orders Starting at 2nd Negative Order] AS
(
SELECT o1.[Customer ID], o1.[Order ID], o1.[OrderDate], o1.[Order Profit],
sum(o1.[Order Profit]) OVER (Partition by o1.[Customer iD]) [Customer Profit Starting at 2nd Unprofitable Order],
Row_Number() OVER (Partition by o1.[Customer ID] Order by [OrderDate] ASC) [2nd Order Rank]
FROM [DataCo Orders] o1
LEFT JOIN [Earliest Unprofitable Orders] o2
ON o1.[Customer ID]=o2.[Customer ID]
WHERE o1.[OrderDate] >= o2.[2nd Negative Order]
)
SELECT [Customer ID], [Order ID], [OrderDate], [Order Profit], [Customer Profit Starting at 2nd Unprofitable Order], [2nd Order Rank],
CASE WHEN [2nd Order Rank] = 1 THEN '2nd Unprofitable Order'
	 WHEN [2nd Order Rank] = 2 THEN '1st Order After'
	 WHEN [2nd Order Rank] = 3 THEN '2nd Order After'
	 WHEN [2nd Order Rank] = 4 THEN '3nd Order After'
	 WHEN [2nd Order Rank] = 5 THEN '4nd Order After'
	 WHEN [2nd Order Rank] = 6 THEN '5th Order After'
	 END AS [2nd Order Ranking]
FROM [Orders Starting at 2nd Negative Order]
WHERE [2nd Order Rank] <= 6
