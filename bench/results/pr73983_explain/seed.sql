insert into dag_bundle(name) values ('bench') on conflict do nothing;
insert into dag (dag_id, max_active_tasks, has_task_concurrency_limits, is_paused, is_stale, max_consecutive_failed_dag_runs,
                 bundle_name, timetable_type, partition_mapper_info, timetable_partitioned)
values ('consumer', 16, false, false, false, 0, 'bench', 'PartitionedAssetTimetable', '{}', true);
-- 99,500 fired partition runs, one dag_run each
insert into dag_run (dag_id, run_id, run_type, run_after, state, partition_key)
select 'consumer', 'p_' || i, 'asset_triggered', now() - (100000 - i) * interval '1 second', 'success', 'acct_' || i
from generate_series(1, 99500) i;
-- 100,000 APDR rows: 99,500 fired (created_dag_run_id set), the newest 500 pending
insert into asset_partition_dag_run (target_dag_id, partition_key, created_at, updated_at, created_dag_run_id)
select 'consumer', 'acct_' || i, now() - (100000 - i) * interval '1 second', now() - (100000 - i) * interval '1 second', dr.id
from generate_series(1, 100000) i
left join dag_run dr on dr.dag_id = 'consumer' and dr.run_id = 'p_' || i;
analyze dag; analyze dag_run; analyze asset_partition_dag_run;
