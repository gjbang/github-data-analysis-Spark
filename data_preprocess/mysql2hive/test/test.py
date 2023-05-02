from datetime import datetime

Local_Format = "%Y-%m-%d %H:%M:%S"

test_time = "2023-05-02 16:11:05"

# convert string to datetime with utc format
test_time_utc = datetime.strptime(test_time, Local_Format).strftime("%Y-%m-%dT%H:%M:%SZ")

print(test_time_utc)
# print(utc)

