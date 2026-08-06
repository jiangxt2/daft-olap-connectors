-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE analytics.events
(
    id UInt64,
    kind Nullable(String),
    score Int32,
    amount Nullable(Decimal(18, 2)),
    event_date Date,
    event_ts DateTime64(6, 'UTC'),
    active Bool,
    payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY id;

INSERT INTO analytics.events VALUES
    (1, 'alpha', 5, 10.25, '2026-01-03', '2026-01-03 01:02:03.123456', true, 'one'),
    (2, 'beta', 15, NULL, '2026-01-10', '2026-01-10 02:03:04.000001', false, 'two'),
    (3, 'alpha', 25, -3.50, '2026-02-02', '2026-02-02 03:04:05.999999', true, 'three'),
    (4, NULL, 35, 0.00, '2026-02-14', '2026-02-14 04:05:06.000000', false, 'four'),
    (5, 'gamma', 45, 999999.99, '2026-03-01', '2026-03-01 05:06:07.100000', true, 'five'),
    (6, 'beta', 55, 1.01, '2026-03-09', '2026-03-09 06:07:08.200000', false, 'six'),
    (7, 'alpha', 65, NULL, '2026-03-20', '2026-03-20 07:08:09.300000', true, 'seven'),
    (8, 'delta', 75, 42.42, '2026-03-31', '2026-03-31 08:09:10.400000', false, 'eight');

CREATE TABLE analytics.empty_events AS analytics.events
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY id;

CREATE TABLE analytics.large_events
(
    id UInt64,
    value String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO analytics.large_events
SELECT number, repeat('x', 64)
FROM numbers(100000);

CREATE TABLE analytics.wide_events
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO analytics.wide_events
SELECT number, repeat('x', 4096)
FROM numbers(16);

CREATE TABLE analytics.type_matrix
(
    id UInt8,
    int8_value Int8,
    int16_value Int16,
    int32_value Int32,
    int64_value Int64,
    uint16_value UInt16,
    uint32_value UInt32,
    uint64_value UInt64,
    float32_value Float32,
    float64_value Float64,
    decimal_value Decimal(18, 2),
    decimal32_value Decimal32(2),
    decimal64_value Decimal64(4),
    decimal128_value Decimal128(6),
    date_value Date,
    date32_value Date32,
    datetime_value DateTime('UTC'),
    datetime64_value DateTime64(6, 'UTC'),
    fixed_value FixedString(4),
    low_cardinality_value LowCardinality(String),
    nullable_value Nullable(String),
    uuid_value UUID,
    ipv4_value IPv4,
    ipv6_value IPv6,
    enum_value Enum8('first' = 1, 'second' = 2),
    enum16_value Enum16('first' = 1, 'second' = 300),
    array_value Array(Int32),
    map_value Map(String, Int32),
    tuple_value Tuple(String, Int32),
    nested_date_values Array(Date),
    semantic_tuple Tuple(observed DateTime('UTC'), request_id UUID)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO analytics.type_matrix VALUES
    (
        1, -8, -1600, -320000, -6400000000, 1600, 320000, 6400000000,
        1.25, -2.5, 12345.67, 12.34, -1234.5678, 123456789.123456,
        '2026-04-01', '1900-01-02', '2026-04-01 01:02:03',
        '2026-04-01 01:02:03.123456', 'test', 'alpha', NULL,
        '12345678-1234-5678-1234-567812345678', '192.0.2.1', '2001:db8::1',
        'second', 'second', [1, 2, 3], map('alpha', 1, 'beta', 2), ('tuple', 7),
        ['2026-04-01', '2026-04-02'],
        ('2026-04-01 01:02:03', '12345678-1234-5678-1234-567812345678')
    );

CREATE TABLE analytics.empty_nested_types
(
    id UInt64,
    nullable_low_cardinality LowCardinality(Nullable(String))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE analytics.unsupported_types
(
    id UInt8,
    wide_decimal_value Decimal(50, 4)
)
ENGINE = MergeTree
ORDER BY id;
