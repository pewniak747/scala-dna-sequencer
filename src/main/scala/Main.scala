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

  private

  def matchLetter(a: Char, b: Char): Boolean = {
    if(a == b) return true

    if (a == 'A') b == 'W' || b == 'R'
    else if (a == 'C') b == 'S' || b == 'Y'
    else if (a == 'T') b == 'W' || b == 'Y'
    else if (a == 'G') b == 'S' || b == 'R'
    else false
  }

}

case class Node(val spectrumWS: Map[Sequence, Count], val spectrumRY: Map[Sequence, Count], val usedWS: Map[Sequence, Int], val usedRY: Map[Sequence, Int], val sequence: Sequence) {

  def isFinished: Boolean =
    (spectrumWS.keySet diff usedWS.keySet).size == 0 && (spectrumRY.keySet diff usedRY.keySet).size == 0

}

class Slave(val kmerLength: Int) extends Actor {
  def receive = {
    case Node(spectrumWS, spectrumRY, usedWS, usedRY, sequence) => {
      val last = sequence.last(kmerLength - 1)
      val filteredWS = spectrumWS.keySet.filter { seq => last.matches(seq.dropLast) }.map { seq => (seq.data.last, seq) }.toMap
      val filteredRY = spectrumRY.keySet.filter { seq => last.matches(seq.dropLast) }.map { seq => (seq.data.last, seq) }.toMap

      for ((letter, ws) <- filteredWS; ry <- filteredRY.get(letter)) yield {
        val nextSequence = Sequence(sequence.data + letter)
        val nextNode = Node(cutSpectrum(spectrumWS, ws), cutSpectrum(spectrumRY, ry), incrementUsed(usedWS, ws), incrementUsed(usedRY, ry), nextSequence)
        sender ! nextNode
      }
    }
  }

  private

  def cutSpectrum(spectrum: Map[Sequence, Count], sequence: Sequence) = {
    spectrum(sequence) match {
      case More => spectrum
      case One => spectrum - sequence
    }
  }

  def incrementUsed(used: Map[Sequence, Int], sequence: Sequence) = {
    used.get(sequence) match {
      case Some(count) => used + ((sequence, count + 1))
      case None => used + ((sequence, 1))
    }
  }
}

class Master(val sequenceLength: Int, val kmerLength: Int, val slaveCount: Int) extends Actor {

  require(sequenceLength > 1)
  require(kmerLength > 1)
  require(slaveCount > 0)

  val slaves = Vector.fill(slaveCount) { context.actorOf(Props(new Slave(kmerLength))) }
  var currentSlave = 0
  var nodesCount: Long = 0

  def receive = {
    case node@Node(_, _, _, _, sequence) if sequence.length == sequenceLength && node.isFinished => {
      println("Found solution: " + sequence)
    }

    case node@Node(_, _, _, _, _) => {
      slaves(currentSlave) ! node
      currentSlave = (currentSlave + 1) % slaveCount
      nodesCount += 1
      context.setReceiveTimeout(1 second)
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

    val in = Source.stdin
    val configLine = """(\d+) (\d+) ([ACTG]+)""".r
    val spectrumLine = """([ACTGWSRY]+) (1|more)""".r

    def addToSpectrums(probe: String, c: String) = {
      val count = if (c == "more") More else One
      if (probe(0) == 'W' || probe(0) == 'S') s1.put(Sequence(probe), count)
      else s2.put(Sequence(probe), count)
    }

    for (line <- Source.stdin.getLines) {
      line match {
        case configLine(sLength, pLength, init) => {
          sequenceLength = sLength.toInt
          probeLength = pLength.toInt
          initial = Sequence(init)
        }
        case spectrumLine(probe, count) => { 
          addToSpectrums(probe, count)
        }
      }
    }

    val initialWS = s1.keys.find { initial.matches(_) }.get
    val initialRY = s2.keys.find { initial.matches(_) }.get

    val actorSystem = ActorSystem()

    val master = actorSystem.actorOf(Props(new Master(sequenceLength, probeLength, 4)))

    master ! Node(s1.toMap, s2.toMap, Map(initialWS -> 1), Map(initialRY -> 1), initial)
  }
}
