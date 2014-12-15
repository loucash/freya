CREATE KEYSPACE freya WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE freya;

CREATE TABLE data_points (
  key blob,
  column1 blob,
  value blob,
  PRIMARY KEY ((key), column1)
) read_repair_chance=0.100000;

CREATE TABLE row_key_index (
  key blob,
  column1 blob,
  value blob,
  PRIMARY KEY ((key), column1)
) read_repair_chance=0.100000;

CREATE TABLE string_index (
  key blob,
  column1 text,
  value blob,
  PRIMARY KEY ((key), column1)
) read_repair_chance=0.100000;
