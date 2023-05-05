DROP TABLE IF EXISTS ods_activitytable;
create external table ods_activitytable (
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

