
db.external_segment_laurent_tests.aggregate(
	{
        $unwind: "$airline"
    },
    {
      $match:
		{year_month: '2016-01', "raw_rec.ORIGIN_COUNTRY_NAME": 'United States', "raw_rec.DEST_COUNTRY_NAME": 'United States'}
	},
	{
	  $group:
	  	{_id: '$airline',
	   		pax: { $sum: "$total_pax" }
		  }
	}
	)
