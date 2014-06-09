package sequencer

import akka.actor._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.mutable
import scala.io.Source

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

  def normalizedWS = normalized(0)

  def normalizedRY = normalized(1)

  private

  def normalized(t: Int) = {
    val newSeq: String = data.dropRight(1).map { l =>
      matchings(l)(t)
    }.mkString("") + data.last
    Sequence(newSeq)
  }

  def matchLetter(a: Char, b: Char): Boolean = {
    if(a == b) return true

    matchings(a) contains b
  }

  val matchings = Map(
    'A' -> List('W', 'R'),
    'C' -> List('S', 'Y'),
    'T' -> List('W', 'Y'),
    'G' -> List('S', 'R')
  )

}

case class Node(val availableWS: Set[Sequence], val availableRY: Set[Sequence], val usedWS: Map[Sequence, Int], val usedRY: Map[Sequence, Int], val sequence: Sequence) {

  def isFinished: Boolean =
    (availableWS diff usedWS.keySet).size == 0 && (availableRY diff usedRY.keySet).size == 0

}

class Slave(val kmerLength: Int, val spectrumWS: Map[Sequence, Count], val spectrumRY: Map[Sequence, Count]) extends Actor {
  def receive = {
    case Node(availableWS, availableRY, usedWS, usedRY, sequence) => {
      val last = sequence.last(kmerLength - 1)
      val potential = basicNucleotides.map { n =>
        val potentialSequence = Sequence(last.data + n)
        (potentialSequence.normalizedWS, potentialSequence.normalizedRY, n)
      }

      for {
        (ws, ry, letter) <- potential if (availableWS.contains(ws) && availableRY.contains(ry))
      } yield {
        val nextSequence = Sequence(sequence.data + letter)
        val nextNode = Node(cutSpectrum(spectrumWS, availableWS, ws), cutSpectrum(spectrumRY, availableRY, ry), incrementUsed(usedWS, ws), incrementUsed(usedRY, ry), nextSequence)
        sender ! nextNode
      }
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

class Master(val sequenceLength: Int, val kmerLength: Int, val spectrumWS: Map[Sequence, Count], val spectrumRY: Map[Sequence, Count], val slaveCount: Int) extends Actor {

  require(sequenceLength > 1)
  require(kmerLength > 1)
  require(slaveCount > 0)

  val slaves = Vector.fill(slaveCount) { context.actorOf(Props(new Slave(kmerLength, spectrumWS, spectrumRY))) }
  var currentSlave = 0
  var nodesCount: Long = 0

  def receive = {
    case node@Node(_, _, _, _, sequence) if sequence.length == sequenceLength && node.isFinished => {
      println("Found solution: " + sequence)
    }

    case node@Node(_, _, _, _, sequence) => {
      slaves(currentSlave) ! node
      currentSlave = (currentSlave + 1) % slaveCount
      nodesCount += 1
      context.setReceiveTimeout(100 milliseconds)
    }

    case ReceiveTimeout => {
      println(s"Searched $nodesCount nodes")
      context.system.shutdown()
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

    val actorSystem = ActorSystem()

    val master = actorSystem.actorOf(Props(new Master(sequenceLength, probeLength, s1.toMap, s2.toMap, 4)))

    master ! Node(s1.keySet.toSet, s2.keySet.toSet, Map(initialWS -> 1), Map(initialRY -> 1), initial)
  }
}
