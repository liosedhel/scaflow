package pl.liosedhel.scaflow.common.model

trait NextVal[A] {
  val id: Int
  val data: A
}
