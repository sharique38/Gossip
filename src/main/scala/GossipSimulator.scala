//package gossipsimulator

import java.util

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import sun.security.provider.certpath.Vertex

import scala.collection.immutable.HashMap
import scala.util.Random
import Array._


case class SpawnWork(nodes:Int, topology:String, algo: String)
case class Rumour(actors:Array[ActorRef], graph:Vector[Vector[Int]], index:Int)
case class PushSum(actors: Array[ActorRef],graph:Vector[Vector[Int]], index:Int, s:Double, w:Double)
case class terminate(index:Int)

class Node(boss: ActorRef, s: Int, w: Int) extends Actor {
  import context._
  var rumourCount:Int = 0
  val maxRumour:Int = 10
  var countReached:Boolean = false
  var sum:Double = s
  var current:Double = 0
  var previous:Double = 0
  var terminateFlag:Boolean = false
  var weight:Double = w
  var countRatioToterminate:Int = 0
  var lastRumourSentTime:Long = 0
  def receive = {
    case Rumour(actors: Array[ActorRef],graph:Vector[Vector[Int]], myindex:Int) =>
      if(rumourCount < maxRumour){
        rumourCount += 1
        /*for(neighbour <- 0 to graph(myindex).size -1){
          actors(graph(myindex)(neighbour)) ! Rumour(actors, graph, graph(myindex)(neighbour))
        }*/
        val groupSize = Random.nextInt(graph(myindex).size) % 3 + 1 // group size 1 to 4
        for(neighbour <- 0 to groupSize) {
          var randomNeighbour: Int = Random.nextInt(graph(myindex).size)
          actors(graph(myindex)(randomNeighbour)) ! Rumour(actors, graph, graph(myindex)(randomNeighbour))
          lastRumourSentTime = System.currentTimeMillis()
        }

      }
      else {
        if( !countReached){
          countReached = true
          boss ! terminate(myindex)
          println("Node " + myindex + " rumour count reached")
        }
      }
    case PushSum(actors: Array[ActorRef],graph:Vector[Vector[Int]], myindex:Int, s:Double, w:Double) =>
      previous = sum/weight
      sum += s
      weight += w
      sum /= 2
      weight /= 2
      current = sum/weight
      if(!terminateFlag)
      {
        if(Math.abs(current-previous) < 0.0000000001)
        {
          countRatioToterminate +=1
          if(countRatioToterminate == 3)
          {
            terminateFlag = true
            boss ! terminate(myindex)
            println("Node " + myindex + " PushSum ratio " + current + " not changing")
            var randomNeighbour: Int = Random.nextInt(graph(myindex).size)
            actors(graph(myindex)(randomNeighbour)) ! PushSum(actors, graph, graph(myindex)(randomNeighbour), 0, 0)
            //println("Node" + myindex + " sending to " + graph(myindex)(randomNeighbour))
          }
          else
          {
            var randomNeighbour: Int = Random.nextInt(graph(myindex).size)
            actors(graph(myindex)(randomNeighbour)) ! PushSum(actors, graph, graph(myindex)(randomNeighbour), sum, weight)
            //println("Node" + myindex + " sending to " + graph(myindex)(randomNeighbour))
          }
        }
        else
        {
          countRatioToterminate = 0
          var randomNeighbour: Int = Random.nextInt(graph(myindex).size)
          actors(graph(myindex)(randomNeighbour)) ! PushSum(actors, graph, graph(myindex)(randomNeighbour), sum, weight)
          //println("Node" + myindex + " sending to " + graph(myindex)(randomNeighbour))
        }
      }
      else
      {
        var randomNeighbour: Int = Random.nextInt(graph(myindex).size)
        actors(graph(myindex)(randomNeighbour)) ! PushSum(actors, graph, graph(myindex)(randomNeighbour), s, w)
        //println("Node" + myindex + " sending to " + graph(myindex)(randomNeighbour))
      }
  }
}

class Boss extends Actor {
  var adjList = Vector[Vector[Int]]()
  var numofNodes:Int = 0
  var topology:String = null
  var algo:String = null
  var actors: Array[ActorRef] = null
  var terminatedNodes:Int = 0
  var startTime:Long = 0
  var endTime:Long = 0
  var percent:Double = 0
  def receive = {
    case SpawnWork(nodes:Int,topo:String,algorithm: String) =>
      numofNodes = nodes
      topology = topo
      algo = algorithm
      actors = new Array[ActorRef](nodes)
      //Create the actors for number of nodes given and store them in an array
      for (i <- 0 to nodes - 1) { // Creating multiple actors
        actors(i) = context.system.actorOf(Props(new Node(self,i+1,1)), i.toString)
      }
      //Build adjacency list
      topology match {
        case "line" =>
          for (i <- 0 to nodes-1){
            var vec = Vector[Int]()
            if(i-1>=0){
              vec = vec :+ (i-1)
            }
            if(i+1<=nodes-1){
              vec = vec :+ (i+1)
            }
            adjList = adjList :+ vec
          }

        //printGraph()
        case "full" =>
          for (i <- 0 to nodes-1){
            var vec = Vector[Int]()
            for (j <- 0 to nodes-1){
              if(j!=i)
                vec = vec :+ j
            }
            adjList = adjList :+ vec
          }
        //printGraph()
        case "3d" =>
          //Fill Cube
          var unique = 0
          var nodeCubeVal = getCubeRoot(nodes)
          numofNodes = nodeCubeVal*nodeCubeVal*nodeCubeVal
          val Cube = ofDim[Int](nodeCubeVal, nodeCubeVal, nodeCubeVal)
          for (i <- 0 to nodeCubeVal - 1) {
            for (j <- 0 to nodeCubeVal - 1) {
              for (k <- 0 to nodeCubeVal - 1) {
                Cube(i)(j)(k) = unique
                unique = unique + 1
              }
            }
          }

          //Print Cube
          /*for (i <- 0 to nodeCubeVal - 1) {
            for (j <- 0 to nodeCubeVal - 1) {
              for (k <- 0 to nodeCubeVal - 1) {
                print(" " + Cube(i)(j)(k))
              }
              print("\n")
            }
            print("\n")
          }*/

          var x = Array(-1, 0, 0, 1, 0, 0)
          var y = Array(0, -1, 0, 0, 1, 0)
          var z = Array(0, 0, -1, 0, 0, 1)

          for (m <- 0 to nodeCubeVal - 1) {
            for (n <- 0 to nodeCubeVal - 1) {
              for (o <- 0 to nodeCubeVal - 1) {
                //Return neighbour
                val f_i = m
                val f_j = n
                val f_k = o

                var vec = Vector[Int]()
                //print("cordinate " + f_i + " " + f_j + " " + f_k + " ")
                //printf("\n")
                var p = 0
                var q = 0
                var r = 0
                for (i <- 0 to 5) {
                  p = f_i + x(i)
                  q = f_j + y(i)
                  r = f_k + z(i)
                  if (p >= 0 && p < nodeCubeVal && q >= 0 && q < nodeCubeVal && r >= 0 && r < nodeCubeVal)
                  {
                    vec = vec :+ Cube(p)(q)(r)
                    //print(" " + Cube(p)(q)(r) + " " + p + " " + q + " " + r + " ")
                  }
                  //print("\n")
                }
                adjList = adjList :+ vec
              }
            }
          }
        // printGraph()

        case "imperfect3d" =>
          //Fill Cube
          var unique = 0
          var nodeCubeVal = getCubeRoot(nodes)
          val Cube = ofDim[Int](nodeCubeVal, nodeCubeVal, nodeCubeVal)
          for (i <- 0 to nodeCubeVal - 1) {
            for (j <- 0 to nodeCubeVal - 1) {
              for (k <- 0 to nodeCubeVal - 1) {
                Cube(i)(j)(k) = unique
                unique = unique + 1
              }
            }
          }

          //Print Cube
          /*for (i <- 0 to nodeCubeVal - 1) {
            for (j <- 0 to nodeCubeVal - 1) {
              for (k <- 0 to nodeCubeVal - 1) {
                print(" " + Cube(i)(j)(k))
              }
              print("\n")
            }
            print("\n")
          }*/

          var x = Array(-1, 0, 0, 1, 0, 0)
          var y = Array(0, -1, 0, 0, 1, 0)
          var z = Array(0, 0, -1, 0, 0, 1)

          for (m <- 0 to nodeCubeVal - 1) {
            for (n <- 0 to nodeCubeVal - 1) {
              for (o <- 0 to nodeCubeVal - 1) {
                //Return neighbour
                val f_i = m
                val f_j = n
                val f_k = o

                var vec = Vector[Int]()
                //print("cordinate " + f_i + " " + f_j + " " + f_k + " ")
                //printf("\n")
                var p = 0
                var q = 0
                var r = 0
                for (i <- 0 to 5) {
                  p = f_i + x(i)
                  q = f_j + y(i)
                  r = f_k + z(i)
                  if (p >= 0 && p < nodeCubeVal && q >= 0 && q < nodeCubeVal && r >= 0 && r < nodeCubeVal)
                  {
                    vec = vec :+ Cube(p)(q)(r)
                    //print(" " + Cube(p)(q)(r) + " " + p + " " + q + " " + r + " ")
                  }
                  //print("\n")
                }
                vec = vec :+ Random.nextInt(numofNodes)
                adjList = adjList :+ vec
              }
            }
          }
      }

      if("gossip".equalsIgnoreCase(algo)){
        actors(numofNodes/2) ! Rumour(actors, adjList, numofNodes/2) //trigger
      }else if("pushsum".equalsIgnoreCase(algo)){
        actors(numofNodes/2) ! PushSum(actors, adjList, numofNodes/2, 0, 0) // trigger
      }
      startTime = System.currentTimeMillis()
      endTime = startTime
    case terminate(index:Int) =>
      terminatedNodes += 1
      percent = terminatedNodes.toDouble *100/numofNodes.toDouble
      //if(System.currentTimeMillis() - endTime > 10000)
      println(Math.round(percent*100.0)/100.0 + "% nodes terminated, nodes left = " + (numofNodes - terminatedNodes) + ", time elapsed = " + (endTime - startTime) + "msec")
      endTime = System.currentTimeMillis()
      if(terminatedNodes == numofNodes){
        endTime = System.currentTimeMillis()
        println("All nodes terminated, total time = " + (endTime - startTime) + "msec, algo = " + algo + ", topology = " +topology)
        context.system.shutdown()
      }

      //update graph by removing terminated node

      /*for( nb <- 0 to adjList(index).size)
      {
        var vec = Vector[Int]()
        for(i <- 0 to adjList(adjList(index)(nb)).size)
        {
          if(adjList(adjList(index)(nb))(i) != index)
          {
            vec = vec :+ adjList(adjList(index)(nb))(i)
          }
        }
        adjList(adjList(index)(nb)) = adjList(adjList(index)(nb)) :+ vec
      }*/

  }

  def getGraph(): Vector[Vector[Int]] ={
    return adjList;
  }
  def printGraph(): Unit ={
    var x:Int = 0;
    var y:Int = 0;
    for(x <- 0 to numofNodes -1){
      print(x + "---> ")
      for(y <- 0 to adjList(x).size - 1){
        print(adjList(x)(y) + " ")
      }
      println(" ")
    }
  }
  def getCubeRoot(n:Int ): Int ={
    var cube = 0
    var i = 0
    while (i*i*i <= n) {
      cube = i*i*i
      i = i + 1
    }
    return i-1;
  }
}

object GossipSimulator extends App{

  if (args.length != 3) {
    println("Wrong Arguments")

  } else {
    //Create the Boss
    var nodes = args(0).toInt
    val topology = args(1)
    val algo = args(2)

    //The number of nodes should be a perfect cube for 3D and imperfect3D topology
    if ("imperfect3D".equalsIgnoreCase(topology) || "3D".equalsIgnoreCase(topology)) {
      nodes = getPerfectCube(nodes)
    }
    val System = ActorSystem("MasterSystem")
    val boss = System.actorOf(Props[Boss],name="Boss")
    boss ! SpawnWork(nodes, topology.toLowerCase, algo.toLowerCase())
  }

  def getPerfectCube(n:Int ): Int ={
    var cube = 0
    var i = 0
    while (i*i*i <= n) {
      cube = i*i*i
      i = i + 1
    }
    return cube;
  }

}
