# MQI

playing around with various spark api.
uses spark streaming, sparkSQL and dataframes

Ingestion app written in spark. Ingests from the following sources: file/mysql/kafka/rabbitmq,

does some simple etl to transform the records into a comma delimited format,
and places them into an existing hive tabke (currently just dumps into the table's location in the hive warehouse)
