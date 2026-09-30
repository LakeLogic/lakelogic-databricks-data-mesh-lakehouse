# Erasure requests

[Back to the README](../README.md)

> Use synthetic subjects in the demo. Review authorisation, target tables and
> scope before processing real requests. A dry run is not completed erasure,
> and lakehouse evidence does not establish deletion from sources or backups.

File a request as a row in the domain's `_lakelogic_erasure_requests` table
(created by setup), then run `pl_privacy_erasure`. LakeLogic owns the
`status`, `processed_at` and `run_id` columns.

```sql
INSERT INTO governed_rideflow_lakehouse_demo.payments._lakelogic_erasure_requests
  (request_id, framework, subject_column, subject_id, requested_at, requested_by, reason, status, system)
VALUES
  ('DSR-2026-001', 'gdpr', 'customer_id', 'cus_123', current_timestamp(), 'privacy@rideflow.example',
   'Art. 17 request', 'pending', 'stripe');
```

With `gdpr_ids` and `hipaa_ids` blank, each system task erases its open requests
and marks them `completed` or `failed`. The job runs dry by default: a dry run marks
requests `dry_run` and leaves them open, so the next run with `dry_run=false` erases
them. Each `_lakelogic_erasure_evidence` row carries the request id in `reason`.
Set `system` so only that system's task takes the request; if it is left blank,
the first task to run closes it for the whole domain.
