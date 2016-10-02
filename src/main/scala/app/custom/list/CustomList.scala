package app.custom.list

import scala.collection._
import scala.collection.generic._

sealed class CustomList[A](seq: A*) extends Traversable[A]
with GenericTraversableTemplate[(A), CustomList]
with TraversableLike[A, CustomList[A]] {
  override def companion = CustomList

  def foreach[U](f: A => U) { seq.foreach(f) }
  override def isEmpty = {
    seq.isEmpty
  }
}

object CustomList extends TraversableFactory[CustomList] {
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, CustomList[A]] = new GenericCanBuildFrom[A]

  def newBuilder[A] = new scala.collection.mutable.LazyBuilder[A, CustomList[A]] {
    def result = {
      val data = parts.foldLeft(List[A]()) { (l, n) => l ++ n }
      new CustomList(data: _*)
    }
  }
}

class PairList[A, B](defaultVal: B, seq: (A, B)*) extends CustomList[(A, B)](seq: _*) {
  def reduceByKey(f: (B, B) => B): PairList[A, B] = {
    PairList(defaultVal, seq.foldLeft(scala.collection.mutable.Map.empty[A, B])((agg, entry) => {
      agg.updated(entry._1, f(entry._2, agg.getOrElse(entry._1, defaultVal)))
    }).toSeq: _*)
  }
}

object PairList {
  def apply[A, B](defaultVal: B,seq: (A, B)*) = {
    new PairList(defaultVal, seq: _*)
  }
}