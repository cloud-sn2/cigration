select from master_add_node(:'worker_1_host', :worker_1_port);
select from master_add_node(:'worker_2_host', :worker_2_port);
select from master_add_node(:'worker_3_host', :worker_3_port);

SET citus.enable_ddl_propagation TO off;
create extension if not exists dblink;
create extension if not exists cigration;
RESET citus.enable_ddl_propagation;