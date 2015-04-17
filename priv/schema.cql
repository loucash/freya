DROP KEYSPACE IF EXISTS freya;

CREATE KEYSPACE freya WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE freya;

CREATE TABLE data_points (
  rowkey blob,
  offset blob,
  value  blob,
  PRIMARY KEY (rowkey, offset)
) WITH read_repair_chance=0.100000;

CREATE TABLE row_key_index (
  metric_idx        blob,
  aggregate_fun     blob,
  aggregate_param1  blob,
  aggregate_param2  blob,
  rowtime           blob,
  rowkey            blob,
  PRIMARY KEY ((metric_idx,
                aggregate_fun,
                aggregate_param1,
                aggregate_param2),
               rowtime,
               rowkey)
) WITH read_repair_chance=0.100000;

CREATE TABLE string_index (
  type blob,
  value text,
  unused blob,
  PRIMARY KEY (type, value)
) WITH read_repair_chance=0.100000;
