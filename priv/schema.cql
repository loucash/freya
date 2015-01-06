DROP KEYSPACE IF EXISTS freya;

CREATE KEYSPACE freya WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE freya;

CREATE TABLE data_points (
  rowkey blob,
  offset blob,
  value blob,
  PRIMARY KEY (rowkey, offset)
) WITH read_repair_chance=0.100000;

CREATE TABLE row_key_index (
  metric_name blob,
  rowkey blob,
  unused blob,
  PRIMARY KEY (metric_name, rowkey)
) WITH read_repair_chance=0.100000;

CREATE TABLE string_index (
  type blob,
  value text,
  unused blob,
  PRIMARY KEY (type, value)
) WITH read_repair_chance=0.100000;
