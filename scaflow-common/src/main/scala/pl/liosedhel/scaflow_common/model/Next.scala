package pl.liosedhel.scaflow_common.model

@SerialVersionUID(1L)
case class Next[A](override val id: Int, override val data: A) extends NextVal[A]
