"""Phase 2 (post mid-season auction) rosters — active from PHASE2_CUTOFF_DATE.

Keyed by the renamed team_name (see rename mapping in main.py lifespan).
Captain (C) = 2x raw_pts; Vice-captain (VC) = 1.5x raw_pts.

Squad size: 16 per team. Top-11 scoring continues.
"""

TEAMS_PHASE2 = {
    "Amal's Team": {
        "players": [
            {"name": "Ashutosh Sharma", "role": "all-rounder", "ipl_team": "Delhi Capitals"},
            {"name": "Praful Hinge", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Glenn Phillips", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
            {"name": "Finn Allen", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "M Shahrukh Khan", "role": "batsman", "ipl_team": "Gujarat Titans"},
            {"name": "Akeal Hosein", "role": "bowler", "ipl_team": "Chennai Super Kings"},
            {"name": "Marco Jansen", "role": "all-rounder", "ipl_team": "Punjab Kings"},
            {"name": "Shivam Dube", "role": "all-rounder", "ipl_team": "Chennai Super Kings"},
            {"name": "Tilak Varma", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "Ishan Kishan", "role": "wicket-keeper", "ipl_team": "Sunrisers Hyderabad", "captain": True},
            {"name": "Dhruv Jurel", "role": "wicket-keeper", "ipl_team": "Rajasthan Royals"},
            {"name": "Will Jacks", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "Rajat Patidar", "role": "batsman", "ipl_team": "Royal Challengers Bengaluru", "vice_captain": True},
            {"name": "Lungi Ngidi", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Nicholas Pooran", "role": "wicket-keeper", "ipl_team": "Lucknow Super Giants"},
            {"name": "Ryan Rickelton", "role": "wicket-keeper", "ipl_team": "Mumbai Indians"},
        ]
    },
    "Prasheel's Team": {
        "players": [
            {"name": "Mohammed Shami", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Nehal Wadhera", "role": "batsman", "ipl_team": "Punjab Kings"},
            {"name": "Prince Yadav", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Shivang Kumar", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Axar Patel", "role": "all-rounder", "ipl_team": "Delhi Capitals", "vice_captain": True},
            {"name": "Angkrish Raghuvanshi", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Abdul Samad", "role": "batsman", "ipl_team": "Lucknow Super Giants"},
            {"name": "Rohit Sharma", "role": "batsman", "ipl_team": "Mumbai Indians"},
            {"name": "Rasikh Salam", "role": "bowler", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Quinton de Kock", "role": "wicket-keeper", "ipl_team": "Mumbai Indians"},
            {"name": "Ashwani Kumar", "role": "bowler", "ipl_team": "Mumbai Indians"},
            {"name": "Sanju Samson", "role": "wicket-keeper", "ipl_team": "Chennai Super Kings", "captain": True},
            {"name": "Noor Ahmad", "role": "bowler", "ipl_team": "Chennai Super Kings"},
            {"name": "Anshul Kamboj", "role": "bowler", "ipl_team": "Chennai Super Kings"},
            {"name": "Mohammed Siraj", "role": "bowler", "ipl_team": "Gujarat Titans"},
            {"name": "Washington Sundar", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
        ]
    },
    "Dhinchak Dudes": {
        "players": [
            {"name": "Yashasvi Jaiswal", "role": "batsman", "ipl_team": "Rajasthan Royals", "vice_captain": True},
            {"name": "Ravi Bishnoi", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Eshan Malinga", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Rinku Singh", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Jacob Bethell", "role": "all-rounder", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Donovan Ferreira", "role": "all-rounder", "ipl_team": "Rajasthan Royals"},
            {"name": "Digvesh Rathi", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Mitchell Starc", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Pat Cummins", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Tim David", "role": "batsman", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Kagiso Rabada", "role": "bowler", "ipl_team": "Gujarat Titans"},
            {"name": "Naman Dhir", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "Mukesh Choudhary", "role": "bowler", "ipl_team": "Chennai Super Kings"},
            {"name": "Priyansh Arya", "role": "batsman", "ipl_team": "Punjab Kings", "captain": True},
            {"name": "Dewald Brevis", "role": "batsman", "ipl_team": "Chennai Super Kings"},
            {"name": "David Miller", "role": "batsman", "ipl_team": "Delhi Capitals"},
        ]
    },
    "Ary-ish 11": {
        "players": [
            {"name": "Manav Suthar", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
            {"name": "Avesh Khan", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Ashok Sharma", "role": "bowler", "ipl_team": "Gujarat Titans"},
            {"name": "Deepak Chahar", "role": "bowler", "ipl_team": "Mumbai Indians"},
            {"name": "Tushar Deshpande", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Mukul Choudhary", "role": "batsman", "ipl_team": "Lucknow Super Giants"},
            {"name": "Suyash Sharma", "role": "bowler", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Sai Sudharsan", "role": "batsman", "ipl_team": "Gujarat Titans", "vice_captain": True},
            {"name": "Shimron Hetmyer", "role": "batsman", "ipl_team": "Rajasthan Royals"},
            {"name": "Suryakumar Yadav", "role": "batsman", "ipl_team": "Mumbai Indians"},
            {"name": "Travis Head", "role": "batsman", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Sherfane Rutherford", "role": "batsman", "ipl_team": "Mumbai Indians"},
            {"name": "Arshdeep Singh", "role": "bowler", "ipl_team": "Punjab Kings"},
            {"name": "Ruturaj Gaikwad", "role": "wicket-keeper", "ipl_team": "Chennai Super Kings"},
            {"name": "Vaibhav Sooryavanshi", "role": "batsman", "ipl_team": "Rajasthan Royals", "captain": True},
            {"name": "Dushmantha Chameera", "role": "bowler", "ipl_team": "Delhi Capitals"},
        ]
    },
    "Rihen": {
        "players": [
            {"name": "Nandre Burger", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Shashank Singh", "role": "all-rounder", "ipl_team": "Punjab Kings"},
            {"name": "Kartik Tyagi", "role": "bowler", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Jacob Duffy", "role": "bowler", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Anukul Roy", "role": "all-rounder", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Sameer Rizvi", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Nitish Kumar Reddy", "role": "all-rounder", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Sandeep Sharma", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Sunil Narine", "role": "all-rounder", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Mitchell Marsh", "role": "all-rounder", "ipl_team": "Lucknow Super Giants"},
            {"name": "Jitesh Sharma", "role": "wicket-keeper", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Jamie Overton", "role": "all-rounder", "ipl_team": "Chennai Super Kings", "captain": True},
            {"name": "Rishabh Pant", "role": "wicket-keeper", "ipl_team": "Lucknow Super Giants"},
            {"name": "Krunal Pandya", "role": "all-rounder", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Bhuvneshwar Kumar", "role": "bowler", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Jos Buttler", "role": "wicket-keeper", "ipl_team": "Gujarat Titans", "vice_captain": True},
        ]
    },
    "Shvetank's 11": {
        "players": [
            {"name": "Phil Salt", "role": "wicket-keeper", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Rovman Powell", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Pathum Nissanka", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Brijesh Sharma", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Mitchell Owen", "role": "batsman", "ipl_team": "Punjab Kings"},
            {"name": "Rachin Ravindra", "role": "all-rounder", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Mayank Yadav", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Ramandeep Singh", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Aniket Verma", "role": "batsman", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Abhishek Sharma", "role": "all-rounder", "ipl_team": "Sunrisers Hyderabad", "captain": True},
            {"name": "Heinrich Klaasen", "role": "wicket-keeper", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Rashid Khan", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
            {"name": "Gurjapneet Singh", "role": "bowler", "ipl_team": "Chennai Super Kings"},
            {"name": "Shubman Gill", "role": "batsman", "ipl_team": "Gujarat Titans", "vice_captain": True},
            {"name": "Corbin Bosch", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "Aiden Markram", "role": "batsman", "ipl_team": "Lucknow Super Giants"},
        ]
    },
    "Prakhar's Team": {
        "players": [
            {"name": "Shreyas Iyer", "role": "batsman", "ipl_team": "Punjab Kings", "captain": True},
            {"name": "Harsh Dubey", "role": "all-rounder", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Xavier Bartlett", "role": "bowler", "ipl_team": "Punjab Kings"},
            {"name": "Matheesha Pathirana", "role": "bowler", "ipl_team": "Kolkata Knight Riders"},
            # Santner traded out 2026-04-28 (kept in roster so his pre-trade P2 points still count)
            {"name": "Mitchell Santner", "role": "all-rounder", "ipl_team": "Mumbai Indians",
             "removed_date": "2026-04-28"},
            {"name": "Lockie Ferguson", "role": "bowler", "ipl_team": "Punjab Kings",
             "added_date": "2026-04-28"},
            {"name": "Yash Raj Punja", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Jaydev Unadkat", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Sakib Hussain", "role": "bowler", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Varun Chakravarthy", "role": "bowler", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Devdutt Padikkal", "role": "batsman", "ipl_team": "Royal Challengers Bengaluru", "vice_captain": True},
            {"name": "Romario Shepherd", "role": "all-rounder", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Shardul Thakur", "role": "bowler", "ipl_team": "Mumbai Indians"},
            {"name": "AM Ghazanfar", "role": "bowler", "ipl_team": "Mumbai Indians"},
            {"name": "Prabhsimran Singh", "role": "wicket-keeper", "ipl_team": "Punjab Kings"},
            {"name": "Marcus Stoinis", "role": "all-rounder", "ipl_team": "Punjab Kings"},
            {"name": "Prasidh Krishna", "role": "bowler", "ipl_team": "Gujarat Titans"},
        ]
    },
    "ESALACUPNAMDE": {
        "players": [
            {"name": "Nitish Rana", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Vijaykumar Vyshak", "role": "bowler", "ipl_team": "Punjab Kings"},
            {"name": "KL Rahul", "role": "wicket-keeper", "ipl_team": "Delhi Capitals", "captain": True},
            {"name": "Ayush Badoni", "role": "batsman", "ipl_team": "Lucknow Super Giants"},
            {"name": "Yuzvendra Chahal", "role": "bowler", "ipl_team": "Punjab Kings"},
            {"name": "Riyan Parag", "role": "batsman", "ipl_team": "Rajasthan Royals"},
            {"name": "Danish Malewar", "role": "batsman", "ipl_team": "Mumbai Indians"},
            {"name": "Hardik Pandya", "role": "all-rounder", "ipl_team": "Mumbai Indians"},
            {"name": "Sarfaraz Khan", "role": "batsman", "ipl_team": "Chennai Super Kings"},
            {"name": "Kuldeep Yadav", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "Ajinkya Rahane", "role": "batsman", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Josh Hazlewood", "role": "bowler", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Cameron Green", "role": "all-rounder", "ipl_team": "Kolkata Knight Riders", "vice_captain": True},
            {"name": "Jasprit Bumrah", "role": "bowler", "ipl_team": "Mumbai Indians"},
            {"name": "Mukesh Kumar", "role": "bowler", "ipl_team": "Delhi Capitals"},
            {"name": "T Natarajan", "role": "bowler", "ipl_team": "Delhi Capitals"},
        ]
    },
    "Dark Horse 11": {
        "players": [
            {"name": "Jofra Archer", "role": "bowler", "ipl_team": "Rajasthan Royals"},
            {"name": "Virat Kohli", "role": "batsman", "ipl_team": "Royal Challengers Bengaluru", "vice_captain": True},
            {"name": "Tristan Stubbs", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Ravindra Jadeja", "role": "all-rounder", "ipl_team": "Rajasthan Royals"},
            {"name": "Josh Inglis", "role": "wicket-keeper", "ipl_team": "Punjab Kings"},
            {"name": "Mohsin Khan", "role": "bowler", "ipl_team": "Lucknow Super Giants"},
            {"name": "Jason Holder", "role": "all-rounder", "ipl_team": "Gujarat Titans"},
            {"name": "Cooper Connolly", "role": "all-rounder", "ipl_team": "Rajasthan Royals", "captain": True},
            {"name": "Tim Seifert", "role": "wicket-keeper", "ipl_team": "Royal Challengers Bengaluru"},
            {"name": "Vaibhav Arora", "role": "bowler", "ipl_team": "Kolkata Knight Riders"},
            {"name": "Kartik Sharma", "role": "all-rounder", "ipl_team": ""},
            {"name": "Lhuan-dre Pretorius", "role": "batsman", "ipl_team": ""},
            {"name": "Himmat Singh", "role": "batsman", "ipl_team": ""},
            {"name": "Salil Arora", "role": "wicket-keeper", "ipl_team": "Sunrisers Hyderabad"},
            {"name": "Prithvi Shaw", "role": "batsman", "ipl_team": "Delhi Capitals"},
            {"name": "Venkatesh Iyer", "role": "all-rounder", "ipl_team": "Royal Challengers Bengaluru"},
        ]
    },
}
