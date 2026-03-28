"""Fantasy team rosters with Captain/VC designations, player roles, and IPL teams."""

TEAMS = {
    "Dark horse 11": {
        "players": [
            {"name": "Abhishek Sharma", "role": "all-rounder", "ipl_team": "Sunrisers Hyderabad", "captain": True},
            {"name": "Shubman Gill", "role": "batsman", "ipl_team": "Gujarat Titans", "vice_captain": True},
            {"name": "Jitesh Sharma", "role": "wicket-keeper", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Cameron Green", "role": "all-rounder", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Ravindra Jadeja", "role": "all-rounder", "ipl_team": "Rajasthan Royals"},
            {"name": "Matt Henry", "role": "bowler", "ipl_team": "Chennai Super Kings"},
            {"name": "Khaleel Ahmed", "role": "bowler", "ipl_team": "Chennai Super Kings"},
            {"name": "Ben Duckett", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Quinton de Kock", "role": "wicket-keeper", "ipl_team": "Mumbai Indians"},
            {"name": "Jacob Bethell", "role": "all-rounder", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Zeeshan Ansari", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Sandeep Sharma", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Nehal Wadhera", "role": "batsman", "ipl_team": "Punjab Kings"},
            {"name": "Nitish Rana", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Kartik Tyagi", "role": "bowler", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Jofra Archer", "role": "bowler", "ipl_team": "Rajasthan Royals"},
        ]
    },
    "Rihen's Team": {
        "players": [
            {"name": "Jasprit Bumrah", "role": "bowler", "ipl_team": "Mumbai Indians"},
            {"name": "Mitchell Marsh", "role": "all-rounder", "ipl_team": "Lucknow Super Giants", "captain": True},
            {"name": "Jos Buttler", "role": "wicket-keeper", "ipl_team": "Gujarat Titans", "vice_captain": True},
            {"name": "Pat Cummins", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Shashank Singh", "role": "all-rounder", "ipl_team": "Punjab Kings"},
            {"name": "Krunal Pandya", "role": "all-rounder", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Tristan Stubbs", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Salil Arora", "role": "wicket-keeper", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "T Natarajan", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Kamindu Mendis", "role": "all-rounder", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Tom Banton", "role": "wicket-keeper", "ipl_team": "Gujarat Titans"},
            {"name": "Rinku Singh", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Ishant Sharma", "role": "bowler", "ipl_team": "Gujarat Titans"},
            {"name": "Karun Nair", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Glenn Phillips", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
            {"name": "Swapnil Singh", "role": "bowler", "ipl_team": "Royal Challengers Bengaluru"},
        ]
    },
    "Prakhar's Team": {
        "players": [
            {"name": "Marco Jansen", "role": "all-rounder", "ipl_team": "Punjab Kings", "vice_captain": True},
            {"name": "Mitchell Starc", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Trent Boult", "role": "bowler", "ipl_team": "Mumbai Indians"},
            {"name": "Ayush Badoni", "role": "batsman", "ipl_team": "Lucknow Super Giants"},
            {"name": "Kuldeep Yadav", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Riyan Parag", "role": "batsman", "ipl_team": "Rajasthan Royals", "captain": True},
            {"name": "Prabhsimran Singh", "role": "wicket-keeper", "ipl_team": "Punjab Kings"},
            {"name": "Mohammed Siraj", "role": "bowler", "ipl_team": "Gujarat Titans"},
            {"name": "Prasidh Krishna", "role": "bowler", "ipl_team": "Gujarat Titans"},
            {"name": "Pathum Nissanka", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Corbin Bosch", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "Mayank Yadav", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Devdutt Padikkal", "role": "batsman", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Washington Sundar", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
            {"name": "Finn Allen", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Vaibhav Arora", "role": "bowler", "ipl_team": "Kolkata Knight Riders"},
        ]
    },
    "Ee Sala Cup Namde FC": {
        "players": [
            {"name": "Sunil Narine", "role": "all-rounder", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Travis Head", "role": "batsman", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Shreyas Iyer", "role": "batsman", "ipl_team": "Punjab Kings"},
            {"name": "Sanju Samson", "role": "wicket-keeper", "ipl_team": "Chennai Super Kings"},
            {"name": "KL Rahul", "role": "wicket-keeper", "ipl_team": "Delhi Capitals"},
            {"name": "Jason Holder", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
            {"name": "Josh Hazlewood", "role": "bowler", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Bhuvneshwar Kumar", "role": "bowler", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Matheesha Pathirana", "role": "bowler", "ipl_team": "Kolkata Knight Riders"},
            {"name": "M Shahrukh Khan", "role": "batsman", "ipl_team": "Gujarat Titans"},
            {"name": "Prashant Veer", "role": "all-rounder", "ipl_team": "Chennai Super Kings"},
        ]
    },
    "Shvetank's Team": {
        "players": [
            {"name": "Phil Salt", "role": "wicket-keeper", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Aiden Markram", "role": "batsman", "ipl_team": "Lucknow Super Giants", "captain": True},
            {"name": "Sai Sudharsan", "role": "batsman", "ipl_team": "Gujarat Titans", "vice_captain": True},
            {"name": "Nicholas Pooran", "role": "wicket-keeper", "ipl_team": "Lucknow Super Giants"},
            {"name": "Heinrich Klaasen", "role": "wicket-keeper", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Rashid Khan", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
            {"name": "Mitchell Santner", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "Akeal Hosein", "role": "bowler", "ipl_team": "Chennai Super Kings"},
            {"name": "Rovman Powell", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Mitchell Owen", "role": "batsman", "ipl_team": "Punjab Kings"},
            {"name": "Donovan Ferreira", "role": "all-rounder", "ipl_team": "Rajasthan Royals"},
            {"name": "Romario Shepherd", "role": "all-rounder", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Avesh Khan", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Nandre Burger", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Jaydev Unadkat", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
        ]
    },
    "Ishan Jindal's Team": {
        "players": [
            {"name": "Ryan Rickelton", "role": "wicket-keeper", "ipl_team": "Mumbai Indians"},
            {"name": "Hardik Pandya", "role": "all-rounder", "ipl_team": "Mumbai Indians", "captain": True},
            {"name": "Arshdeep Singh", "role": "bowler", "ipl_team": "Punjab Kings"},
            {"name": "Nitish Kumar Reddy", "role": "all-rounder", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Rohit Sharma", "role": "batsman", "ipl_team": "Mumbai Indians"},
            {"name": "Vaibhav Sooryavanshi", "role": "batsman", "ipl_team": "Rajasthan Royals", "vice_captain": True},
            {"name": "Marcus Stoinis", "role": "all-rounder", "ipl_team": "Punjab Kings"},
            {"name": "Kwena Maphaka", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Ashutosh Sharma", "role": "all-rounder", "ipl_team": "Delhi Capitals"},
            {"name": "Tushar Deshpande", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Dushmantha Chameera", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Shivam Mavi", "role": "all-rounder", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Abishek Porel", "role": "wicket-keeper", "ipl_team": "Delhi Capitals"},
            {"name": "Mayank Markande", "role": "bowler", "ipl_team": "Mumbai Indians"},
            {"name": "Azmatullah Omarzai", "role": "all-rounder", "ipl_team": "Punjab Kings"},
        ]
    },
    "Amal's Team": {
        "players": [
            {"name": "Shimron Hetmyer", "role": "batsman", "ipl_team": "Rajasthan Royals"},
            {"name": "Suryakumar Yadav", "role": "batsman", "ipl_team": "Mumbai Indians", "vice_captain": True},
            {"name": "Dhruv Jurel", "role": "wicket-keeper", "ipl_team": "Rajasthan Royals"},
            {"name": "Ajinkya Rahane", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Tilak Varma", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "Shivam Dube", "role": "all-rounder", "ipl_team": "Chennai Super Kings"},
            {"name": "Axar Patel", "role": "all-rounder", "ipl_team": "Delhi Capitals"},
            {"name": "Ishan Kishan", "role": "wicket-keeper", "ipl_team": "Sunrisers Hyderabad", "captain": True},
            {"name": "Ajay Mandal", "role": "all-rounder", "ipl_team": "Delhi Capitals"},
            {"name": "Anrich Nortje", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Abdul Samad", "role": "batsman", "ipl_team": "Lucknow Super Giants"},
            {"name": "Manish Pandey", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Rahul Tewatia", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
            {"name": "Shubham Dubey", "role": "batsman", "ipl_team": "Rajasthan Royals"},
            {"name": "Naman Dhir", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "David Miller", "role": "batsman", "ipl_team": "Delhi Capitals"},
        ]
    },
    "Prasheel super 11": {
        "players": [
            {"name": "Virat Kohli", "role": "batsman", "ipl_team": "Royal Challengers Bengaluru", "captain": True},
            {"name": "Angkrish Raghuvanshi", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Dewald Brevis", "role": "batsman", "ipl_team": "Chennai Super Kings"},
            {"name": "Sherfane Rutherford", "role": "batsman", "ipl_team": "Mumbai Indians"},
            {"name": "Rajat Patidar", "role": "batsman", "ipl_team": "Royal Challengers Bengaluru", "vice_captain": True},
            {"name": "Harshal Patel", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Will Jacks", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "Gurnoor Brar", "role": "bowler", "ipl_team": "Gujarat Titans"},
            {"name": "Noor Ahmad", "role": "bowler", "ipl_team": "Chennai Super Kings"},
            {"name": "Lungi Ngidi", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Venkatesh Iyer", "role": "all-rounder", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Mukesh Kumar", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Digvesh Rathi", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Suyash Sharma", "role": "bowler", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Mohammed Shami", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Mukesh Choudhary", "role": "bowler", "ipl_team": "Chennai Super Kings"},
        ]
    },
    "Dhinchak Dudes": {
        "players": [
            {"name": "Yashasvi Jaiswal", "role": "batsman", "ipl_team": "Rajasthan Royals", "vice_captain": True},
            {"name": "Priyansh Arya", "role": "batsman", "ipl_team": "Punjab Kings"},
            {"name": "Rishabh Pant", "role": "wicket-keeper", "ipl_team": "Lucknow Super Giants"},
            {"name": "Ruturaj Gaikwad", "role": "wicket-keeper", "ipl_team": "Chennai Super Kings", "captain": True},
            {"name": "Varun Chakravarthy", "role": "bowler", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Arshad Khan", "role": "bowler", "ipl_team": "Gujarat Titans"},
            {"name": "Prithvi Shaw", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Tim David", "role": "batsman", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Adam Milne", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Yuzvendra Chahal", "role": "bowler", "ipl_team": "Punjab Kings"},
            {"name": "Deepak Chahar", "role": "bowler", "ipl_team": "Mumbai Indians"},
            {"name": "Ravi Bishnoi", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Vipraj Nigam", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Sai Kishore", "role": "bowler", "ipl_team": "Gujarat Titans"},
            {"name": "Mangesh Yadav", "role": "all-rounder", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Ayush Mhatre", "role": "all-rounder", "ipl_team": "Chennai Super Kings"},
        ]
    },
}


def get_player_names() -> list[str]:
    """Get flat list of all player names."""
    names = []
    for team_data in TEAMS.values():
        for p in team_data["players"]:
            names.append(p["name"])
    return names


def get_captain_vc() -> dict:
    """Get captain/vc info: player_name -> 'C' or 'VC'."""
    result = {}
    for team_data in TEAMS.values():
        for p in team_data["players"]:
            if p.get("captain"):
                result[p["name"]] = "C"
            elif p.get("vice_captain"):
                result[p["name"]] = "VC"
    return result


def get_player_meta() -> dict:
    """Get player metadata: player_name -> {role, ipl_team, designation}."""
    result = {}
    for team_name, team_data in TEAMS.items():
        for p in team_data["players"]:
            result[p["name"]] = {
                "role": p.get("role", ""),
                "ipl_team": p.get("ipl_team", ""),
                "designation": "C" if p.get("captain") else "VC" if p.get("vice_captain") else "",
                "fantasy_team": team_name,
            }
    return result
