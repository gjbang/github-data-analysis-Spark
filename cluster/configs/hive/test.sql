DROP TABLE IF EXISTS test_json_data;
create external table test_json_data (
  id string,
  type string,
  actor map<string, string>,
  repo map<string, string>,
  payload map<string, string>,
  public string,
  created_at string
)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
stored as textfile;

load data inpath '/testlog/*.json' into table test_json_data;
