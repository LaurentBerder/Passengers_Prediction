var start = new Date();
start.setHours(0,0,0,0);

var end = new Date();
end.setHours(23,59,59,999);

db.external_segment_tmp.find({'inserted': {'$gte': start, '$lt': end}});