package com.evolutiongaming

package object skafka {

  type ClientId = String

  type Partition = Int

  object Partition {
    val Min: Partition = 0
  }


  type Offset = Long

  object Offset {
    val Min: Offset = 0l
  }


  type Topic = String

  
  type Metadata = String

  object Metadata {
    val Empty: Metadata = ""
  }


  type Bytes = Array[Byte]

  object Bytes {
    val Empty: Bytes = Array.empty
  }
}