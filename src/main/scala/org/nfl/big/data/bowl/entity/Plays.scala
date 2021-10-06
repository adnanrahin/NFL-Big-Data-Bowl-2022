package org.nfl.big.data.bowl.entity

case class Plays(
                  gameId: String,
                  playId: String,
                  playDescription: String,
                  quarter: String,
                  down: String,
                  yardsToGo: String,
                  possessionTeam: String,
                  specialTeamsPlayType: String,
                  specialTeamsResult: String,
                  kickerId: String,
                  returnerId: String,
                  kickBlockerId: String,
                  yardLineSide: String,
                  yardLineNumber: String,
                  gameClock: String,
                  penaltyCodes: String,
                  penaltyJerseyNumbers: String,
                  penaltyYards: String,
                  preSnapHomeScore: String,
                  preSnapVisitorScore: String,
                  passResult: String,
                  kickLength: String,
                  kickReturnYardage: String,
                  playResult: String,
                  absoluteYardLineNumber: String

                )
