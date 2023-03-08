# hubsdataengproject

Insights on data.
-----------------
=> Parquet file contains 20,000 records where each row is considered as part.
=>18 colums that gives entity characteristic about parts namely -created,updated,queued,geometric_heuristics,holes,job_run_time,latheability,machining_directions,multipart,neighbors,poles,sheet_like_shape,unmachinable_edges,extrusion_height,units,status,time,uuid
=>Gathering insights from holes column which has multivalue Json data contianing keys such as center,direction,end1,end2,facecount,length,radius.


Objective:
-----------
If holes data satisfies the below two conditions then update bool values to newly created columns.

1. 𝑙𝑒𝑛𝑔𝑡ℎ > 𝑟𝑎𝑑𝑖𝑢𝑠 * 2 * 10 (𝑝𝑜𝑜𝑟 𝑟𝑎𝑡𝑖𝑜)    => has_unreachable_hole_warning
2. 𝑙𝑒𝑛𝑔𝑡ℎ > 𝑟𝑎𝑑𝑖𝑢𝑠 * 2 * 40 (𝑐𝑟𝑖𝑡𝑖𝑐𝑎𝑙 𝑟𝑎𝑡𝑖𝑜)  => has_unreacheable_hole_error

Results :
-----------
Parts with poor ratio in holes data are ,     has_unreachable_hole_warning = 99
Parts with critical ratio in holes data are , has_unreacheable_hole_error  = 17

Assumptions :
--------------
1.If one of the key in holes json satisfies above condition I have assumed the part is either poor ratio or critical ratio.
2.Considered <NA> missing data in holes row as [0,0] in a list.






