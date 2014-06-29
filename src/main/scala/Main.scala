package sequencer

import scala.collection.mutable
import scala.io.Source
import scala.compat.Platform

trait Count
object One extends Count
object More extends Count

case class Sequence(val data: String) {

  def drop: Sequence = Sequence(data.drop(1))

  def dropLast = Sequence(data.dropRight(1))

  def last(n: Int) = Sequence(data.takeRight(n))

  def length = data.length

  def matches(other: Sequence): Boolean = {
    other.length == length && data.zip(other.data).forall { case (a, b) => matchLetter(a, b) }
  }

  def append(other: Sequence) = Sequence(data + other.data)

  def append(letter: Char) = Sequence(data + letter)

  def normalizedWS = normalized(0)

  def normalizedRY = normalized(1)

  private

  def normalized(t: Int) = {
    val newSeq: String = data.map { l =>
      matchings(l)(t)
    }.mkString("")
    Sequence(newSeq)
  }

  def matchLetter(a: Char, b: Char): Boolean = {
    if(a == b) return true

    matchings(a) contains b
  }

  val matchings = Map(
    'A' -> Array('W', 'R'),
    'C' -> Array('S', 'Y'),
    'T' -> Array('W', 'Y'),
    'G' -> Array('S', 'R')
  )

}

case class Node(val availableWS: Set[Sequence], val availableRY: Set[Sequence], val usedWS: Map[Sequence, Int], val usedRY: Map[Sequence, Int], val sequence: Sequence, val normalized: (Sequence, Sequence)) {

  def isFinished: Boolean =
    (availableWS diff usedWS.keySet).size == 0 && (availableRY diff usedRY.keySet).size == 0

  def allowMove(ws: Sequence, ry: Sequence): Boolean =
    availableWS.contains(ws) && availableRY.contains(ry)

  def isAllowed(spectrumWS: Map[Sequence, Count], spectrumRY: Map[Sequence, Count], left: Int) = true
    //isAllowedSpectrum(spectrumWS, availableWS, usedWS, left) && isAllowedSpectrum(spectrumRY, availableRY, usedRY, left)

  private

  def isAllowedSpectrum(spectrum: Map[Sequence, Count], available: Set[Sequence], used: Map[Sequence, Int], left: Int) = {
    val needed = available.toList.map { seq => spectrum(seq) match {
      case More if used contains seq => if(used(seq) < 2) 1 else 0
      case More => 2
      case _ => 1
    } }.sum
    needed <= left
  }

}

class Solver(val kmerLength: Int, val sequenceLength: Int, val spectrumWS: Map[Sequence, Count], val spectrumRY: Map[Sequence, Count]) {

  def solve(node: Node): Iterator[Node] = node match {
    case Node(availableWS, availableRY, usedWS, usedRY, sequence, (normalizedWS, normalizedRY)) => {

      val moves = basicNucleotides.map { n =>
        (normalizedWS.drop append n, normalizedRY.drop append n)
      }

      (for {
        (ws, ry) <- moves if node.allowMove(ws, ry)
      } yield {
        val lastLetter = Sequence(ws.data.last.toString)
        val nextSequence = sequence append lastLetter
        val nextNormalizedWS = normalizedWS.drop append lastLetter.normalizedWS
        val nextNormalizedRY = normalizedRY.drop append lastLetter.normalizedRY
        Node(cutSpectrum(spectrumWS, availableWS, ws), cutSpectrum(spectrumRY, availableRY, ry), incrementUsed(usedWS, ws), incrementUsed(usedRY, ry), nextSequence, (nextNormalizedWS, nextNormalizedRY))
      }).filter { nextNode =>
        if (nextNode.isAllowed(spectrumWS, spectrumRY, sequenceLength - sequence.length))
          true
        else {
          println("CUT at depth " + sequence.length)
          false
        }
      }.toIterator
    }
  }

  private

  def cutSpectrum(spectrum: Map[Sequence, Count], available: Set[Sequence], sequence: Sequence) = {
    spectrum(sequence) match {
      case More => available
      case One => available - sequence
    }
  }

  def incrementUsed(used: Map[Sequence, Int], sequence: Sequence) = {
    used.get(sequence) match {
      case Some(count) => used + ((sequence, count + 1))
      case None => used + ((sequence, 1))
    }
  }

  val basicNucleotides = List('A', 'C', 'T', 'G')
}

class Sequencer(val sequenceLength: Int, val kmerLength: Int, val spectrumWS: Map[Sequence, Count], val spectrumRY: Map[Sequence, Count]) {

  require(sequenceLength > 1)
  require(kmerLength > 1)

  var nodesCount: Long = 0
  var solutionsCount: Long = 0
  val startTime = Platform.currentTime
  val solver = new Solver(kmerLength, sequenceLength, spectrumWS, spectrumRY)

  def start(node: Node) = {
    solve(node)
    finished
  }

  def finished = {
    val time = (Platform.currentTime - startTime) / 1000.0
    val wsOnes = spectrumWS.values.filter { _ == One }.size
    val wsMores = spectrumWS.values.filter { _ == More }.size
    val ryOnes = spectrumRY.values.filter { _ == One }.size
    val ryMores = spectrumRY.values.filter { _ == More }.size

    println("-----")
    println("Finished processing")
    println(s"Sequence length: $sequenceLength")
    println(s"Probe length: $kmerLength")
    println(s"Spectrum WS ones: $wsOnes")
    println(s"Spectrum WS mores: $wsMores")
    println(s"Spectrum RY ones: $ryOnes")
    println(s"Spectrum RY mores: $ryMores")
    println(s"Found solutions: $solutionsCount")
    println(s"Searched nodes: $nodesCount")
    println(s"Elapsed time: $time")
  }

  def solve(node: Node): Unit = {
    val sequence = node.sequence
    if (sequence.length == sequenceLength) {
      if (node.isFinished) {
        println("Found solution: " + sequence)
        solutionsCount += 1
      }
    } else {
      nodesCount += 1
      if(nodesCount % 100000 == 0) println(s"Searched $nodesCount nodes so far...")
      val nextNodes = solver solve node
      nextNodes.foreach { node => solve(node) }
    }
  }
}

object Main {
  def main(args: Array[String]) = {
    println("Starting sequencer...")

    val s1 = mutable.Map[Sequence, Count]()
    val s2 = mutable.Map[Sequence, Count]()
    var initial = Sequence("")
    var sequenceLength = 0
    var probeLength = 0
    var currentCount: Count = One

    val in = Source.stdin
    val initialLine = """;INFO\|([ACGT]+)\|(\d+)""".r
    val spectrumLine = """;BINARY-(WS|RY)\|(\d+)""".r
    val countLine = """>(1|N)""".r
    val probeLine = """([WSRY]+[ACTG])""".r

    def addToSpectrums(probe: String, count: Count) = {
      if (probe(0) == 'W' || probe(0) == 'S') s1.put(Sequence(probe), count)
      else s2.put(Sequence(probe), count)
    }

    for (line <- Source.stdin.getLines) {
      line match {
        case initialLine(_initial, _sequenceLength) => {
          sequenceLength = _sequenceLength.toInt
          initial = Sequence(_initial)
        }
        case spectrumLine(_, _probeLength) => { 
          probeLength = _probeLength.toInt + 1
        }
        case countLine(_count) => {
          currentCount = if (_count == "N") More else One
        }
        case probeLine(_probe) => {
          addToSpectrums(_probe, currentCount)
        }
        case _ => { }
      }
    }

    val initialWS = s1.keys.find { initial.matches(_) }.get
    val initialRY = s2.keys.find { initial.matches(_) }.get

    val sequencer = new Sequencer(sequenceLength, probeLength, s1.toMap, s2.toMap)

    sequencer start Node(s1.keySet.toSet, s2.keySet.toSet, Map(initialWS -> 1), Map(initialRY -> 1), initial, (initial.normalizedWS, initial.normalizedRY))
  }
}
