package pl.liosedhel.scaflow

import java.util.UUID

trait PersistentWorkflowTests {
  def uniqueId() = UUID.randomUUID().toString
  def uniqueId(name: String) = name + UUID.randomUUID().toString
}
