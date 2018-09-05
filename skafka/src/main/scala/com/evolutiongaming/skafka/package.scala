package com.evolutiongaming

package object skafka {

  type Partition = Int

  type Offset = Long

  object Offset {
    val Min: Offset = 0
  }
  
  type Topic = String

  type Metadata = String


  type Bytes = Array[Byte]

  object Bytes {
    val Empty: Bytes = Array.empty
  }
}