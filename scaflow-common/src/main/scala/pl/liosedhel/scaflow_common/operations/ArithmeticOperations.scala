package pl.liosedhel.scaflow_common.operations

import scala.util.Random

object ArithmeticOperations {

  object Pow extends Function[Int, Int] with Serializable {
    def apply(x: Int): Int = {
      x * x
    }
  }

  object Sum extends Function[Seq[Int], Int] with Serializable {
    def apply(x: Seq[Int]): Int = {
      x.sum
    }
  }

  object BuggyTransformation extends Function[Int, Int] with Serializable {
    def apply(x: Int): Int = {
      if (Random.nextDouble() < 0.8) throw new Exception("Bum") else x
    }
  }

}
