--Filter First
REGISTER  file:/home/hadoop/lib/pig/piggybank.jar;
--as we want to run it on 10 workers we set default_parallel is 10
SET default_parallel 10;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
--Load the contents of flight data in FlightData
FlightData = LOAD 's3://ankitamr/data.csv' USING CSVLoader();

--load the FlightData as Flights1
Flights1 = FOREACH FlightData GENERATE (int)$0 AS yearf1,(int)$2 AS monthf1,$5 AS flightDatef1,$11 As originf1,$17 As destf1,$35 AS arrTimef1, $37 AS arrDelayminsf1, (float)$41 AS cancelledf1, (float)$43 AS divertedf1;

--load the FlightData as Flights2
Flights2 = FOREACH FlightData GENERATE (int)$0 AS yearf2,(int)$2 AS monthf2,$5 AS flightDatef2,$11 As originf2,$17 As destf2,$24 AS depTimef2,$35 AS arrTimef2,$37 AS arrDelayminsf2, (float)$41 AS cancelledf2, (float)$43 AS divertedf2;

--filter records in Flights1 based on origin, cancelled and diverted
filterFlight1 = FILTER Flights1 BY (originf1=='ORD' AND cancelledf1==0.00 AND divertedf1 == 0.00);

--filter records in Flights2 based on destination,cancelled and diverted
filterFlight2 = FILTER Flights2 BY (destf2=='JFK'AND cancelledf2 ==0.00 AND divertedf2 == 0.00);

--now filter the records check that the flight date of Flights1 is in range
finalFilter1 = FILTER filterFlight1 BY (monthf1 >=6 AND monthf1<=12 AND yearf1==2007) OR (monthf1>=1 AND monthf1<=5 AND yearf1 ==2008);

--also filter the records check that the flight date of Flights2 is in range
finalFilter2 = FILTER filterFlight2 BY (monthf2 >=6 AND monthf2<=12 AND yearf2==2007) OR (monthf2>=1 AND monthf2<=5 AND yearf2 ==2008);

--join records based on Flight date and Destination of records in Flights1
--join records based on Flight date and origin of records in Flights2
join_records = JOIN finalFilter1 BY (flightDatef1,destf1), finalFilter2 BY (flightDatef2,originf2);

--Filter the join records where departure time of flights in Flights2 is after the arrival time of flights in Flights1
filtered_on_join_records = FILTER join_records BY depTimef2 > arrTimef1;

--compute the delay for each pair S1 and S2 in Flights1 and Flights2 respectively
averageDelay = FOREACH filtered_on_join_records GENERATE arrDelayminsf1 + arrDelayminsf2 as val;

--group the average delays for all pairs together, to calculate total delay
groupDelays_on_records = GROUP averageDelay ALL;
totalaverageDelay = FOREACH groupDelays_on_records GENERATE AVG(averageDelay.val);
--store the total average delay in a file OutputFilterFirst
STORE totalaverageDelay INTO 's3://ankitamr/OutputFilterFirst';
