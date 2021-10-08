package org.nfl.big.data.bowl.dataloader

trait DataLoader {
  def loadRDD(): Any
}
