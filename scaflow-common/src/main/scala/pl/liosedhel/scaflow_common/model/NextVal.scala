package pl.liosedhel.scaflow_common.model

trait NextVal[A] {
  val id: Int
  val data: A
}
