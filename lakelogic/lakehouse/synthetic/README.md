# Synthetic data generator bundle (LakeLogic Synthetic v2)

Source: medallion design bp:snap-d91da9d87d0e48ba r1
Generated at: revision:chv1:e7b0c49f07bce8fda7e01b76564ac41c80ddcbb956e46bec30c967061acb4e1a seal:sha256:5dec3512859b80333097a50f1f3bd620ff4ec644a86a89f7ebd2a821ba9cc108
Generator: lakelogic DataGenerator v1.79.2

## Fidelity profile: contract-derived

This bundle ships a **deterministic seeded generator**, not synthetic rows. It was built
from the contract **schemas only** — **no production rows were read** — so it can be shared
freely: no synthetic data crosses a boundary until *you* run `generate.py` locally.

Values are contract-derived: the generator uses semantic hints from column names (email,
name, phone, address, ...) and type-correct fakes otherwise. This is NOT profile- or
sample-informed fidelity (those are later, separately-governed modes); no distribution or
value from your production data informs these fakes.

## Permanent synthetic classification

Every artifact produced by this bundle is **synthetic, permanently** (ADR-0011 decision 6:
synthetic is not the same as anonymised). Do not re-classify generated data as production.

## PII / masked columns

Columns flagged `pii` / `sensitive` in the contracts carry a `masking` strategy. Run the
generated frames through `lakelogic.DataProcessor(<contract>).run(df)` to apply masking by
construction (`redact` -> `***REDACTED***`, `hash` -> pseudonym). The generator emits
semantically-shaped fakes for these columns; it never emits a real identifier.

## Staged rollout (use synthetic BEFORE real data)

Run any exported pipeline against THIS synthetic data first, staged:
`dry_run` -> `ddl_only` -> `live` — validate the design on synthetic before pointing it at
production. Injected-invalid rows (`invalid_ratio=0.0`) let you prove your
quality gates quarantine bad rows.

## Contents

  - `generate.py` — the standalone generator (needs `pip install lakelogic`)
  - `config.json` — the pinned config (seed, rows, window, relationships)
  - `contracts/*.yaml` — the contracts (deterministically serialized, once; the canonical source of truth)

## Config

  - seed: 7
  - rows_per_table: 100
  - invalid_ratio: 0.0
  - window: 2020-01-01T00:00:00 .. 2025-01-01T00:00:00 (module default)

## Tables

  - bronze_checkr_background_checks
  - bronze_checkr_driver_licences
  - bronze_google_ads_campaigns
  - bronze_google_analytics_app_events
  - bronze_google_analytics_sessions
  - bronze_hubspot_email_campaigns
  - bronze_hubspot_push_notifications
  - bronze_internal_city_master
  - bronze_internal_dim_cancellation_reason_seed
  - bronze_internal_dim_country_seed
  - bronze_internal_dim_currency_seed
  - bronze_internal_dim_date_seed
  - bronze_internal_dim_payment_method_seed
  - bronze_internal_dim_surge_tier_seed
  - bronze_internal_dim_vehicle_type_seed
  - bronze_internal_fx_rates_daily
  - bronze_meta_ads_campaigns
  - bronze_rideflow_driver_profiles
  - bronze_rideflow_driver_telemetry
  - bronze_rideflow_rider_app_events
  - bronze_rideflow_rider_profiles
  - bronze_rideflow_trip_cancellations
  - bronze_rideflow_trip_completed
  - bronze_rideflow_trip_requests
  - bronze_stripe_charges
  - bronze_stripe_payouts
  - bronze_stripe_refunds
  - bronze_twilio_sms_logs
  - bronze_zendesk_support_tickets
  - gold_dim_adjudication
  - gold_dim_background_checks_status
  - gold_dim_cancelled_by
  - gold_dim_channel
  - gold_dim_charges_status
  - gold_dim_city_master
  - gold_dim_date
  - gold_dim_device_category
  - gold_dim_device_os
  - gold_dim_dim_cancellation_reason_seed
  - gold_dim_dim_country_seed
  - gold_dim_dim_currency_seed
  - gold_dim_dim_date_seed
  - gold_dim_dim_payment_method_seed
  - gold_dim_dim_surge_tier_seed
  - gold_dim_dim_vehicle_type_seed
  - gold_dim_direction
  - gold_dim_driver_licences
  - gold_dim_driver_profiles
  - gold_dim_driver_telemetry_status
  - gold_dim_entry_point
  - gold_dim_payment_method
  - gold_dim_payment_method_type
  - gold_dim_payouts_status
  - gold_dim_preferred_payment_method
  - gold_dim_priority
  - gold_dim_refund_reason
  - gold_dim_refunds_reason
  - gold_dim_refunds_status
  - gold_dim_result
  - gold_dim_rider_profiles
  - gold_dim_screen_name
  - gold_dim_sms_logs_status
  - gold_dim_support_tickets_status
  - gold_dim_trip_type
  - gold_fact_app_events
  - gold_fact_background_checks
  - gold_fact_charges
  - gold_fact_driver_telemetry
  - gold_fact_email_campaigns
  - gold_fact_fx_rates_daily
  - gold_fact_payouts
  - gold_fact_push_notifications
  - gold_fact_refunds
  - gold_fact_rider_app_events
  - gold_fact_sessions
  - gold_fact_sms_logs
  - gold_fact_support_tickets
  - gold_fact_trip_cancellations
  - gold_fact_trip_completed
  - gold_fact_trip_requests
  - gold_google_ads_fact_campaigns
  - gold_google_analytics_dim_event_name
  - gold_meta_ads_fact_campaigns
  - gold_rideflow_dim_city_code
  - gold_rideflow_dim_event_name
  - gold_rideflow_dim_platform
  - silver_checkr_background_checks
  - silver_checkr_driver_licences
  - silver_google_ads_campaigns
  - silver_google_analytics_app_events
  - silver_google_analytics_sessions
  - silver_hubspot_email_campaigns
  - silver_hubspot_push_notifications
  - silver_internal_city_master
  - silver_internal_dim_cancellation_reason_seed
  - silver_internal_dim_country_seed
  - silver_internal_dim_currency_seed
  - silver_internal_dim_date_seed
  - silver_internal_dim_payment_method_seed
  - silver_internal_dim_surge_tier_seed
  - silver_internal_dim_vehicle_type_seed
  - silver_internal_fx_rates_daily
  - silver_meta_ads_campaigns
  - silver_rideflow_driver_profiles
  - silver_rideflow_driver_telemetry
  - silver_rideflow_rider_app_events
  - silver_rideflow_rider_profiles
  - silver_rideflow_trip_cancellations
  - silver_rideflow_trip_completed
  - silver_rideflow_trip_requests
  - silver_stripe_charges
  - silver_stripe_payouts
  - silver_stripe_refunds
  - silver_twilio_sms_logs
  - silver_zendesk_support_tickets
