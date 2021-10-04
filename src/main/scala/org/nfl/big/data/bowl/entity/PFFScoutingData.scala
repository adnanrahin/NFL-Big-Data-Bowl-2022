package org.nfl.big.data.bowl.entity

case class PFFScoutingData(
                            gameId: String,
                            playId: String,
                            snapDetail: String,
                            snapTime: String,
                            operationTime: String,
                            hangTime: String,
                            kickType: String,
                            kickDirectionIntended: String,
                            kickDirectionActual: String,
                            returnDirectionIntended: String,
                            returnDirectionActual: String,
                            missedTackler: String,
                            assistTackler: String,
                            tackler: String,
                            kickoffReturnFormation: String,
                            gunners: String,
                            puntRushers: String,
                            specialTeamsSafeties: String,
                            vises: String,
                            kickContactType: String
                          )
