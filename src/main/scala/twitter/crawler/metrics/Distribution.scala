package twitter.crawler.metrics

import collection.mutable

class Distribution[T](counters: mutable.Map[T, Int] = mutable.Map.empty[T, Int]) {
  def clear = {
    counters.clear()
  }
  var total = 0;

  def increment(key: T, value: Int = 1) = {
    val counter = counters.getOrElse(key, 0)
    counters(key) = counter + value
  }

  def entropy(): Double = {
    var partialEntropy = 0.0
    for (counter <- counters.values){
      total+=counter
      partialEntropy += counter * log2(counter)
    }
    log2(total) - partialEntropy/total
  }

  def merge[K](f: T => K): Distribution[K]={
    val keyF: ((T, Int)) => K = {
      case (key, value) => f(key)
    }
    new Distribution[K](mutable.Map(counters.groupBy[K](keyF).mapValues(map => map.values.sum).toSeq: _*) )
  }

  override def toString: String={
    var result = ""
    counters foreach {
      case (key, value) =>
      result += "%s: %d \n".format(key, value)
    }
    result
  }
}
