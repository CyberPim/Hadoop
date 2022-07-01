-- CSV Extension
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

-- load the file from maria_dev 
orderList = LOAD '/user/maria_dev/files/orders.csv' 
USING CSVExcelStorage() AS
(game_id:int,
unit_id:int,
unit_order:chararray,
location:chararray,
target:chararray,
target_dest:chararray,
success:int,
reason:int,
turn_num:int);


-- Get rows filterd by Holland
filterdList = FILTER orderList BY target == 'Holland';

-- Group by location
groupByLocation = GROUP filterdList BY (location, target);

-- Count the rows
countRows = FOREACH groupByLocation GENERATE group, COUNT(filterdList);

-- Order the list 
orderedList = ORDER countRows BY $0 ASC;

-- show result
DUMP orderedList;