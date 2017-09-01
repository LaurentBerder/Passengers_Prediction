db.external_segment_laurent_tests.aggregate(
   [
   	{
        $unwind: "$year_month"
    },
      {  $group : {
           
           _id: "$provider",
           from: { $min: "$year_month"},
           to: { $max: "$year_month"},
           nb_airlines: {$addToSet: "$airline"},
           segments: { $sum: NumberInt(1) },	
           pax: { $sum: "$total_pax" }
                   } 
      }
   ]
)