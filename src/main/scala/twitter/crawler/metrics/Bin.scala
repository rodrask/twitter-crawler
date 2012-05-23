package twitter.crawler.metrics

import collection.immutable.SortedSet

class Bin(var start: Long, var end: Long) {
  var onTimestamp: Long = -1
  def isOn = onTimestamp > 0
  def check(history: SortedSet[Long]): Boolean = {
    history.range(start, end).lastOption match {
      case None =>
        onTimestamp = -1
        false
      case Some(ts) =>
        onTimestamp = ts
        true
    }
  }

  def move(distance: Long)={
    start += distance
    end += distance
  }

  def moveToChange(history: SortedSet[Long]): Long={
    if (isOn){
       onTimestamp - start + 1
    }
    else{
      history.from(end).headOption match {
        case None =>
          Long.MaxValue
        case Some(ts)=>
          ts - end + 1
      }
    }
  }

}
