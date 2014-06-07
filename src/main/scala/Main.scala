package sequencer

import akka.actor._
import scala.concurrent.duration._
import scala.collection.mutable
import scala.io.Source

case class Count
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

  def matchLetter(a: Char, b: Char) = {
    if (a == 'A') b == 'W' || b == 'R'
    else if (a == 'C') b == 'S' || b == 'Y'
    else if (a == 'T') b == 'W' || b == 'Y'
    else if (a == 'G') b == 'S' || b == 'R'
    else false
  }

}

case class Node(val spectrumWS: Map[Sequence, Count], val spectrumRY: Map[Sequence, Count], val sequence: Sequence)

class Slave(val kmerLength: Int) extends Actor {
  def receive = {
    case Node(spectrumWS, spectrumRY, sequence) => {
      val last = sequence.last(kmerLength - 1)
      val filteredWS = spectrumWS.keySet.filter { seq => last.matches(seq.dropLast) }.map { seq => (seq.data.last, seq) }.toMap
      val filteredRY = spectrumRY.keySet.filter { seq => last.matches(seq.dropLast) }.map { seq => (seq.data.last, seq) }.toMap
      filteredWS.keySet.foreach { letter =>
        for (ws <- filteredWS.get(letter); ry <- filteredRY.get(letter)) yield {
          val nextSequence = Sequence(sequence.data + letter)
          sender ! Node(cutSpectrum(spectrumWS, ws), cutSpectrum(spectrumRY, ry), nextSequence)
        }
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
}

class Master(val sequenceLength: Int, val kmerLength: Int, val slaveCount: Int) extends Actor {

  require(sequenceLength > 1)
  require(kmerLength > 1)
  require(slaveCount > 0)

  val slaves = Vector.fill(slaveCount) { context.actorOf(Props(new Slave(kmerLength))) }
  var currentSlave = 0

  def receive = {
    case Node(_, _, sequence) if sequence.length == sequenceLength => {
      println("Found solution: " + sequence)
    }

    case node@Node(_, _, _) => {
      slaves(currentSlave) ! node
      currentSlave = (currentSlave + 1) % slaveCount
      context.setReceiveTimeout(1 second)
    }

    case ReceiveTimeout => { exit() }
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

    val actorSystem = ActorSystem()

    val master = actorSystem.actorOf(Props(new Master(sequenceLength, probeLength, 4)))

    master ! Node(s1.toMap, s2.toMap, initial)
  }
}
