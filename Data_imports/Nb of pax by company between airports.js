db.segment_initial_data.aggregate(
    {
	$match : {origin : "LHR", destination: "GIG", year_month: "2016-01"}
    },
    {
	$group : { _id : "$operating_airline", pax : { $sum : "$passengers" } }
    }
  );





db.external_segment_laurent_tests.aggregate(
    {
	$match : {origin : "LHR", destination: "GIG", year_month: "2016-01"}
    },
    {
	$group : { _id : "$airline", pax : { $sum : "$total_pax" } }
    }
  );
