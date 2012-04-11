package twitter.crawler.metrics

import collection.immutable.SortedSet

class Pattern(startTs: Long, intervals: Array[Long]) {
  var borders: Array[Long] = intervals.scanLeft(startTs)(_ + _)

  def move(duration: Long)={
    borders = borders map (b => b + duration)
  }

  def observe(history: SortedSet[Long]): Array[Byte]={
    borders.sliding(2).map(interval => hasBetween(interval(0), interval(1), history)).toArray
  }

  def hasBetween(from: Long, to: Long, history: SortedSet[Long]): Byte={
    if (history.range(from, to).size > 0) 1 else 0
  }

  def minMove(history: SortedSet[Long]): Long ={
    borders.map(b => history.from(b + 1).head - b).min
  }

  def rightBorder = borders.last

}
