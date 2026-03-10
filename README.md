# Data Task For Rotate: State of the Sky
This document explains what has been done for the Rotate data analysis assignment, and gets its name from the answer to Question 3.

## How to Run (Concerns Part 3 of the assignment)

 ### Option A: Quick Run                                                                             
 ```bash             
   cd web         
   python -m http.server 8000                                                                
 ``` 
 Open: http://127.0.0.1:8000                                                                                    
 This uses pre-generated files in:                                                       
 - web/data/hourly_stats.json                                                             
 - web/data/snapshots/*.geojson                                                                                                                                         
 ### Option B: Rebuild dashboard data from raw inputs                                   
 Requirements:                                                                   
 - Python 3.10+                                                                       
 - duckdb package                                                                            
 ```bash                                                                                
   pip install duckdb  
   python src/state_of_the_sky.py --date 2022-10-03                                                   
   # or to include all days:     
   # python src/state_of_the_sky.py --all-days                                                                                                                       
 ```                                                                             
 Then run:                                                                             
 ```bash                                                                                 
   cd web                                                                                
   python -m http.server 8000 
```
## Why DuckDB?



## Explicit Answers to Questions
### Question 1

I loaded the raw CSV files into DuckDB and created a cleaned table for analysis, which has explicit data types. 
* Dates are stored as DATE, times as TIME, and are combined into event times as a TIMESTAMP.
* Identifiers such as airport and aircraft codes are left as VARCHAR.
* Quantitative fields such as altitude, latitude, and longitude are cast to DOUBLE
* Flight ID is a BIGINT.

These choises of datatypes make filtering, joins, aggregations, and general analyses easier than working with raw CSV strings directly.

### Question 2

First, the event-level table was converted into a flight-level table by selecting one representative row per `light_id`, because multiple rows in the source data correspond to different events of the same flight (I did not choose a specific flight event as it might be missing). Each flight was then joined with the airplane details JSON using the aircraft equipment code.

**Note:** The resulting capacity is of course the maximum aircraft-type capacity, not actual cargo loaded on that flight as I cannot predict that when not given the passenger load, how much of the capacity is used up by fuel etc. (For fuel specifically I need *average fuel burn* and calculate what is needed on the given flight route).

Leisure (small) aircraft like the Cessna 172 (C172) and the Piper PA28 (P28A) have very high flight volumes yet they do not carry any cargo (at least not more than a couple of suitcases). These were left with `capacity = null`, as assuming 0 would also be incorrect.

The JSON for `airplane_details` contains duplicate `code_iaco` values for different aircraft (`DC10` for both MD10 and D10-10F, `T204` for Tu-204 / Tu-214 / Tu-204 Freighter, `L101` for both passenger and freighter varieties of the Tristar). This required me to do detuping before the join with the CSV derived flight data to get the capacity table. The JSON also fails to give units for the `payload` and `volume` fields, but I am assuming `kg` and `m^3` respectively.

To account for some models not having data for `volume`, I have also added a `match_status` column which can assume one of the following 4 values: `matched_full` for models that have `volume` data, `matched_no_volume` for those who don't, `missing_equipment` for flights with no specified equipment, and `no_data_on_equipment` for flights that use equipment without capacity data in `airplane_details`.

#### Data Quality Summary
The pipeline creates `capacity_data_quality_summary`, which reports counts and percentages of flights by quality category (`total_flights`, `matched_full`, `matched_no_volume`, `missing_equipment`, `no_data_on_equipment`). A copy of this can be seen below:

   | Metric | Flights | % of Flights |                          
   |--------|--------:|---------------:|
   | total_flights        | 202407  | 100.00         |                                                       
   | matched_full         | 105461  | 52.10          |                                                    
   | matched_no_volume    | 625     | 0.31           |                                                    
   | missing_equipment    | 3586    | 1.77           |                                             
   | no_data_on_equipment | 92735   | 45.82 |


### Question 3



**Disclaimer:** Parts of the webpage produced for this section of the assignment have been vibe-coded.
