CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
    sensorreadingtime BIGINT,
    serialnumber STRING,
    distancefromobject BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://udacity-landing-zone-bucket/step_trainer_landing/';