package twitter.crawler.metrics

import collection.immutable.SortedSet

class Pattern(startTs: Long, intervals: List[Long]) {
  var borders: List[Long] = intervals.scanLeft(startTs)(_ + _)
  def rightBorder = borders(borders.size - 2)

  def move(duration: Long) = {
    borders = borders map (b => b + duration)
  }

  def observe(history: SortedSet[Long]): List[Byte] = {
    borders.sliding(2).map(interval => hasBetween(interval(0), interval(1), history)).toList
  }

  def hasBetween(from: Long, to: Long, history: SortedSet[Long]): Byte = {
    if (history.range(from, to).size > 0) 1 else 0
  }

  def minMove(history: SortedSet[Long]): Long = {
    val bMinMove: Long => Long = {
      b: Long =>
        val iter = history.from(b)
        if (iter.isEmpty)
          0l
        else
          iter.head - b
    }
    borders.map(bMinMove).min + 1
  }

  def isInside(to: Long)={
    borders.last <=  to+1
  }


}
