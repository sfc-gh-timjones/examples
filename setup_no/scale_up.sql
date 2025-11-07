

create or replace warehouse my_compute_warehouse
warehouse_size = xsmall 
auto_suspend = 30
comment = 'Create a warehouse to execute below queries';

use warehouse my_compute_warehouse;

--alter warehouse my_compute_warehouse set warehouse_size = small; 
--alter warehouse my_compute_warehouse set warehouse_size = medium; 


--NOTE: the snowflake_sample_data tpch data is shared by default to all Snowflake accounts. 
use schema snowflake_sample_data.tpch_sf100;   --tpch_sf10 | tpch_sf100 | tpch_sf1000

alter session set use_cached_result = FALSE;

select count(*)
from lineitem;

select
       l_returnflag,
       l_linestatus,
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as sum_base_price,
       sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
       sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
       avg(l_quantity) as avg_qty,
       avg(l_extendedprice) as avg_price,
       avg(l_discount) as avg_disc,
       count(*) as count_order
 from
       lineitem
 where
       l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
 group by
       l_returnflag,
       l_linestatus
 order by
       l_returnflag,
       l_linestatus;





--cleanup
drop warehouse if exists my_compute_warehouse;




       