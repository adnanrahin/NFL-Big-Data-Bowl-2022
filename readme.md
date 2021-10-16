ummary of data
The 2022 Big Data Bowl data contains Next Gen Stats player tracking, play, game, player, and PFF scouting data for all 2018-2020 Special Teams plays. Here, you'll find a summary of each data set in the 2022 Data Bowl, a list of key variables to join on, and a description of each variable.

File descriptions
Game data: The games.csv contains the teams playing in each game. The key variable is gameId.

Play data: The plays.csv file contains play-level information from each game. The key variables are gameId and playId.

Player data: The players.csv file contains player-level information from players that participated in any of the tracking data files. The key variable is nflId.

Tracking data: Files tracking[season].csv contain player tracking data from season [season]. The key variables are gameId, playId, and nflId.

PFF Scouting data: The PFFScoutingData.csv file contains play-level scouting information for each game. The key variables are gameId and playId.

### Game data
1. gameId: Game identifier, unique (numeric)
2. season: Season of game
3. week: Week of game
4. gameDate: Game Date (time, mm/dd/yyyy)
5. gameTimeEastern: Start time of game (time, HH:MM:SS, EST)
6. homeTeamAbbr: Home team three-letter code (text)
7. visitorTeamAbbr: Visiting team three-letter code (text)
### Play data
1. gameId: Game identifier, unique (numeric)
2. playId: Play identifier, not unique across games (numeric)
3. playDescription: Description of play (text)
4. quarter: Game quarter (numeric)
5. down: Down (numeric)
6. yardsToGo: Distance needed for a first down (numeric)
7. possessionTeam: Team punting, placekicking or kicking off the ball (text)
8. specialTeamsPlayType: Formation of play: Extra Point, Field Goal, Kickoff or Punt (text)
9. specialTeamsPlayResult: Special Teams outcome of play dependent on play type: Blocked Kick Attempt, Blocked Punt, Downed, Fair Catch, Kick Attempt Good, Kick Attempt No Good, Kickoff Team Recovery, Muffed, Non-Special Teams Result, Out of Bounds, Return or Touchback (text)
10. kickerId: nflId of placekicker, punter or kickoff specialist on play (numeric)
11. returnerId: nflId(s) of returner(s) on play if there was a special teams return. Multiple returners on a play are separated by a ; (text)
12. kickBlockerId: nflId of blocker of kick on play if there was a blocked field goal or blocked punt (numeric)
13. yardlineSide: 3-letter team code corresponding to line-of-scrimmage (text)
14. yardlineNumber: Yard line at line-of-scrimmage (numeric)
15. gameClock: Time on clock of play (MM:SS)
16. penaltyCodes: NFL categorization of the penalties that occurred on the play. A standard penalty code followed by a d means the penalty was on the defense. Multiple penalties on a play are separated by a ; (text)
17. penaltyJerseyNumber: Jersey number and team code of the player committing each penalty. Multiple penalties on a play are separated by a ; (text)
18. penaltyYards: yards gained by possessionTeam by penalty (numeric)
19. preSnapHomeScore: Home score prior to the play (numeric)
20. preSnapVisitorScore: Visiting team score prior to the play (numeric)
21. passResult: Scrimmage outcome of the play if specialTeamsPlayResult is "Non-Special Teams Result" (C: Complete pass, I: Incomplete pass, S: Quarterback sack, IN: Intercepted pass, R: Scramble, ' ': Designed Rush, text)
22. kickLength: Kick length in air of kickoff, field goal or punt (numeric)
23. kickReturnYardage: Yards gained by return team if there was a return on a kickoff or punt (numeric)
24. playResult: Net yards gained by the kicking team, including penalty yardage (numeric)
25. absoluteYardlineNumber: Location of ball downfield in tracking data coordinates (numeric)
###Player data
1. gameId: Game identifier, unique (numeric)
2. playId: Play identifier, not unique across games (numeric)
3. nflId: Player identification number, unique across players (numeric)
4. Height: Player height (text)
5. Weight: Player weight (numeric)
6. birthDate: Date of birth (YYYY-MM-DD)
7. collegeName: Player college (text)
8. Position: Player position (text)
9. displayName: Player name (text)
10. Tracking data

### Files tracking[season].csv contains player tracking data from season [season].

1. time: Time stamp of play (time, yyyy-mm-dd, hh:mm:ss)
2. x: Player position along the long axis of the field, 0 - 120 yards. See Figure 1 below. (numeric)
3. y: Player position along the short axis of the field, 0 - 53.3 yards. See Figure 1 below. (numeric)
4. s: Speed in yards/second (numeric)
5. a: Speed in yards/second^2 (numeric)
6. dis: Distance traveled from prior time point, in yards (numeric)
7. o: Player orientation (deg), 0 - 360 degrees (numeric)
8. dir: Angle of player motion (deg), 0 - 360 degrees (numeric)
9. event: Tagged play details, including moment of ball snap, pass release, pass catch, tackle, etc (text)
10. nflId: Player identification number, unique across players (numeric)
11. displayName: Player name (text)
12. jerseyNumber: Jersey number of player (numeric)
13. position: Player position group (text)
14. team: Team (away or home) of corresponding player (text)
15. frameId: Frame identifier for each play, starting at 1 (numeric)
16. gameId: Game identifier, unique (numeric)
17. playId: Play identifier, not unique across games (numeric)
18. playDirection: Direction that the offense is moving (left or right)
### PFF Scouting data
1. gameId: Game identifier, unique (numeric)
2. playId: Play identifier, not unique across games (numeric)
3. snapDetail: On Punts, whether the snap was on target and if not, provides detail (H: High, L: Low, <: Left, >: Right, OK: Accurate Snap, text)
4. operationTime: Timing from snap to kick on punt plays in seconds: (numeric)
5. hangTime: Hangtime of player's punt or kickoff attempt in seconds. Timing is taken from impact with foot to impact with the ground or a player. (numeric)
6. kickType: Kickoff or Punt Type (text).
7. Possible values for kickoff plays:
   1. D: Deep - your normal deep kick with decent hang time
   2. F: Flat - different than a Squib in that it will have some hang time and no roll but has a lower trajectory and hang time than a Deep kick off
   3. K: Free Kick - Kick after a safety
   4. O: Obvious Onside - score and situation dictates the need to regain possession. Also the hands team is on for the returning team
   5. P: Pooch kick - high for hangtime but not a lot of distance - usually targeting an upman
   6. Q: Squib - low-line drive kick that bounces or rolls considerably, with virtually no hang time
   7. S: Surprise Onside - accounting for score and situation an onsides kick that the returning team doesn’t expect. Hands teams probably aren't on the field
   8. B: Deep Direct OOB - Kickoff that is aimed deep (regular kickoff) that goes OOB directly (doesn't bounce)
8. Possible values for punt plays:
   1. N: Normal - standard punt style
   2. R: Rugby style punt
   3. A: Nose down or Aussie-style punts
9. kickDirectionIntended: Intended kick direction from the kicking team's perspective - based on how coverage unit sets up and other factors (L: Left, R: Right, C: Center, text).
10. kickDirectionActual: Actual kick direction from the kicking team's perspective (L: Left, R: Right, C: Center, text).
11. returnDirectionIntended: The return direction the punt return or kick off return unit is set up for from the return team's perspective (L: Left, R: Right, C: Center, text).
12. returnDirectionActual: Actual return direction from the return team's perspective (L: Left, R: Right, C: Center, text).
13. missedTacklers: Jersey number and team code of player(s) charged with a missed tackle on the play. It will be reasonable to assume that he should have brought down the ball carrier and failed to do so. This situation does not have to entail contact, but it most frequently does. Missed tackles on a QB by a pass rusher are also included here. Multiple missed tacklers on a play are separated by a ; (text).
14. assistTacklers: Jersey number and team code of player(s) assisting on the tackle. Multiple assist tacklers on a play are separated by a ; (text).
15. tacklers: Jersey number and team code of player making the tackle (text).
16. kickoffReturnFormation: 3 digit code indicating the number of players in the Front Wall, Mid Wall and Back Wall (text).
17. gunners: Jersey number and team code of player(s) lined up as gunner on punt unit. Multiple gunners on a play are separated by a ; (text).
18. puntRushers: Jersey number and team code of player(s) on the punt return unit with "Punt Rush" role for actively trying to block the punt. Does not include players crossing the line of scrimmage to engage in punt coverage players in a "Hold Up" role. Multiple punt rushers on a play are separated by a ; (text).
19. specialTeamsSafeties: Jersey number and team code for player(s) with "Safety" roles on kickoff coverage and field goal/extra point block units - and those not actively advancing towards the line of scrimmage on the punt return unit. Multiple special teams safeties on a play are separated by a ; (text).
20. vises: Jersey number and team code for player(s) with a "Vise" role on the punt return unit. Multiple vises on a play are separated by a ; (text).
21. kickContactType: Detail on how a punt was fielded, or what happened when it wasn't fielded (text).
 Possible values:
    1. BB: Bounced Backwards
    2. BC: Bobbled Catch from Air
    3. BF: Bounced Forwards
    4. BOG: Bobbled on Ground
    5. CC: Clean Catch from Air
    6. CFFG: Clean Field From Ground
    7. DEZ: Direct to Endzone
    8. ICC: Incidental Coverage Team Contact
    9. KTB: Kick Team Knocked Back
    10. KTC: Kick Team Catch
    11. KTF: Kick Team Knocked Forward
    12. MBC: Muffed by Contact with Non-Designated Returner
    13. MBDR: Muffed by Designated Returner
    14. OOB: Directly Out Of Bounds
