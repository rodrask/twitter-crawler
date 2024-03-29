package twitter.crawler.metrics

import scala.math.min
import collection.immutable.{BitSet, SortedSet}
import com.codahale.logula.Logging

class Process(points: SortedSet[Long]) extends Logging {
  def distributionFor(bins: BinSequence): Distribution[BitSet] = {
    val result = new Distribution[BitSet]()
    while (!bins.finished) {
      val bSet = bins.count(points)
      val distanceForChangeCount = bins.minMove(points)
      result.increment(bSet, distanceForChangeCount.toInt - 1)
      bins.move(distanceForChangeCount)
    }
    result
  }
}

class JoinedProcesses(mainPoints: SortedSet[Long], additionalPoints: SortedSet[Long]) extends Logging {
  def joinedDistribution(mainBins: BinSequence, additionalBins: BinSequence): (Distribution[BitSet], Distribution[BitSet]) = {
    val joinedDistribution = new Distribution[BitSet]()
    val singleDistribution = new Distribution[BitSet]()
    val shift = additionalBins.bins.size
    while (!mainBins.finished) {
      val additionalSet = additionalBins.count(additionalPoints)
      val mainSet = mainBins.count(mainPoints, shift)
      val distanceForChangeCount = min(additionalBins.minMove(additionalPoints), mainBins.minMove(mainPoints))
      joinedDistribution.increment(additionalSet | mainSet, distanceForChangeCount.toInt)
      singleDistribution.increment(mainSet, distanceForChangeCount.toInt)
      mainBins.move(distanceForChangeCount)
      additionalBins.move(distanceForChangeCount)
    }
    (singleDistribution, joinedDistribution)
  }
}

object JoinedProcesses {
  def borders(mainPoints: SortedSet[Long], additionalPoints: SortedSet[Long]) = {
    val begin = min(mainPoints.head, additionalPoints.head)
    val end = mainPoints.last+1
    (begin, end)
  }

  def bias(total: Int, length: Int): Double={
    ((1 << length-1) - 1)/(2.0*total*LN2)
  }

  def calculateIT(fromProcess: SortedSet[Long], toProcess: SortedSet[Long]) = {
    val (begin, end): (Long, Long) = borders(toProcess, fromProcess)
    val mainBins = new BinSequence(begin, end, INTERVALS)
    val additionalBins = new BinSequence(begin, end, ADDITIONAL_INTERVALS)

    val last = INTERVALS.size + ADDITIONAL_INTERVALS.size - 1
    val dropLast: BitSet => BitSet = {
      bs => bs - last
    }
    val (single, joined) = new JoinedProcesses(toProcess, fromProcess).joinedDistribution(mainBins, additionalBins)

    val singleConditionalEntropy = single.entropy() - single.merge(dropLast).entropy()
    val singleBias = bias(single.total, INTERVALS.size)

    val joinedConditionalEntropy = joined.entropy() - joined.merge(dropLast).entropy()
    val joinedBias = bias(joined.total, INTERVALS.size + ADDITIONAL_INTERVALS.size)
    singleConditionalEntropy - joinedConditionalEntropy
  }
}

