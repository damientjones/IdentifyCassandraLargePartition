DROP TABLE IF EXISTS partition_metrics;

CREATE TABLE IF NOT EXISTS partition_metrics (
	table_name text,
	token_value text,
	key text,
	rec_count bigint,
	size_mb bigint,
	time timestamp,
	PRIMARY KEY (table_name, token_value)
) WITH compression = {
	'sstable_compression' : 'LZ4Compressor'
}
AND compaction = {
	'class' : 'SizeTieredCompactionStrategy'
};