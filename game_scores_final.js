db.games.aggregate([
    {
        $lookup:{
			localField: "SID",   
            from: "stadium",       
            foreignField: "SID", 
            as: "game_stadium"         
        }
		
    }, 
	{  $unwind:"$game_stadium" },
	{
		$lookup:{  
				from: "teams",
				localField: "TeamID1", 				
				foreignField: "TeamID", 
				as: "team_1"         
			}
	},
	{   $unwind:"$team_1" },
	{
		$lookup:{  
				from: "teams",
				localField: "TeamID2", 				
				foreignField: "TeamID", 
				as: "team_2"         
			}
	},
			
	{   $unwind:"$team_2" },
	
	{
		$project:{
			"team_1.Team": 1,
			"team_2.Team": 1,
			"team_1.TeamID": 1,
			"team_2.TeamID": 1,
			"game_stadium.SCity": 1,
			"game_stadium.SName": 1,
			team_1_Score: 1,
			team_2_Score: 1,
			MatchDate: 1
			
		}
	},
	{$out : "match_team"}
]);

db.teams.aggregate([
		{
			$lookup:{  
					from: "match_team",
					let:{t_id1:"$team_1.TeamID",t_id2:"$team_2.TeamID"},
					pipeline:[
								{
									$match:{
											$expr:{
													$or:[
															{
																$eq:["$TeamID","$$t_id1"]
																
															},
															{
																$eq:["$TeamID","$$t_id2"]
																
															}
														]
													}	
											}
								}
							],
					as: "match_games"         
				}
		},

				
		{   $unwind:"$match_games" },
		{
			"$redact":{
					"$cond": [
						{$or:[
						{"$eq":["$TeamID","$match_games.team_1.TeamID"]},
						{"$eq":["$TeamID","$match_games.team_2.TeamID"]}
					]},
					"$$KEEP",
					"$$PRUNE"
					]
			}
		},
		{
			$group:{_id:"$Team",object:{$push:{
												teamName1:"$match_games.team_1.Team",
												teamName2:"$match_games.team_2.Team",
												teamScore1:"$match_games.team_1_Score",
												teamScore2:"$match_games.team_2_Score",
												matchDate:"$match_games.MatchDate",
												stadiumName:"$match_games.game_stadium.SName",
												stadiumCity:"$match_games.game_stadium.SCity"
											}
										}
					},
		
		},
		{$out : "game_scores"}
	]).pretty();