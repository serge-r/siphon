CREATE TABLE IF NOT EXISTS siphon_flow_kafka
(
    `sequence` UInt32,
    `observation_domain` UInt32,
    `export_time` String,
    `received_at` String,
    `exporter` String,
    `source_ip` String,
    `source_name` Nullable(String),
    `source_static_name` Nullable(String),
    `source_inventory_name` Nullable(String),
    `source_namespace` Nullable(String),
    `source_pod_name` Nullable(String),
    `source_aws_service` Nullable(String),
    `source_aws_region` Nullable(String),
    `source_dns_name` Nullable(String),
    `source_geoip_isp` Nullable(String),
    `source_geoip_asn` Nullable(UInt32),
    `source_geoip_country` Nullable(String),
    `source_geoip_city` Nullable(String),
    `source_port` UInt16,
    `destination_ip` String,
    `destination_name` Nullable(String),
    `destination_static_name` Nullable(String),
    `destination_inventory_name` Nullable(String),
    `destination_namespace` Nullable(String),
    `destination_pod_name` Nullable(String),
    `destination_aws_service` Nullable(String),
    `destination_aws_region` Nullable(String),
    `destination_dns_name` Nullable(String),
    `destination_geoip_isp` Nullable(String),
    `destination_geoip_asn` Nullable(UInt32),
    `destination_geoip_country` Nullable(String),
    `destination_geoip_city` Nullable(String),
    `destination_port` UInt16,
    `protocol` UInt8,
    `packets` UInt64,
    `bytes` UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:9094',
    kafka_topic_list = 'siphon-flows',
    kafka_group_name = 'siphon-flows',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS siphon_flow
(
    `sequence` UInt32,
    `observation_domain` UInt32,
    `export_time` DateTime64(9, 'UTC'),
    `received_at` DateTime64(9, 'UTC'),
    `exporter` String,
    `source_ip` String,
    `source_name` Nullable(String),
    `source_static_name` Nullable(String),
    `source_inventory_name` Nullable(String),
    `source_namespace` Nullable(String),
    `source_pod_name` Nullable(String),
    `source_aws_service` Nullable(String),
    `source_aws_region` Nullable(String),
    `source_dns_name` Nullable(String),
    `source_geoip_isp` Nullable(String),
    `source_geoip_asn` Nullable(UInt32),
    `source_geoip_country` Nullable(String),
    `source_geoip_city` Nullable(String),
    `source_port` UInt16,
    `destination_ip` String,
    `destination_name` Nullable(String),
    `destination_static_name` Nullable(String),
    `destination_inventory_name` Nullable(String),
    `destination_namespace` Nullable(String),
    `destination_pod_name` Nullable(String),
    `destination_aws_service` Nullable(String),
    `destination_aws_region` Nullable(String),
    `destination_dns_name` Nullable(String),
    `destination_geoip_isp` Nullable(String),
    `destination_geoip_asn` Nullable(UInt32),
    `destination_geoip_country` Nullable(String),
    `destination_geoip_city` Nullable(String),
    `destination_port` UInt16,
    `protocol` UInt8,
    `packets` UInt64,
    `bytes` UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(received_at)
ORDER BY (received_at, exporter, source_ip, destination_ip, source_port, destination_port)
TTL received_at + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS siphon_flow_mv
TO siphon_flow
AS SELECT
    `sequence`,
    `observation_domain`,
    parseDateTime64BestEffort(`export_time`, 9, 'UTC') AS `export_time`,
    parseDateTime64BestEffort(`received_at`, 9, 'UTC') AS `received_at`,
    `exporter`,
    `source_ip`,
    `source_name`,
    `source_static_name`,
    `source_inventory_name`,
    `source_namespace`,
    `source_pod_name`,
    `source_aws_service`,
    `source_aws_region`,
    `source_dns_name`,
    `source_geoip_isp`,
    `source_geoip_asn`,
    `source_geoip_country`,
    `source_geoip_city`,
    `source_port`,
    `destination_ip`,
    `destination_name`,
    `destination_static_name`,
    `destination_inventory_name`,
    `destination_namespace`,
    `destination_pod_name`,
    `destination_aws_service`,
    `destination_aws_region`,
    `destination_dns_name`,
    `destination_geoip_isp`,
    `destination_geoip_asn`,
    `destination_geoip_country`,
    `destination_geoip_city`,
    `destination_port`,
    `protocol`,
    `packets`,
    `bytes`
FROM siphon_flow_kafka
WHERE length(_error) = 0;

CREATE TABLE siphon_flow_kafka_errors
(
    raw_message String,
    error String,
    received_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY received_at;

CREATE MATERIALIZED VIEW siphon_flow_kafka_errors_mv
TO siphon_flow_kafka_errors
AS SELECT
    _raw_message AS raw_message,
    _error AS error
FROM siphon_flow_kafka
WHERE length(_error) > 0;
