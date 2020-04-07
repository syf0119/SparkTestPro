object Test {
  def main(args: Array[String]): Unit = {
    val map=Map(1->"hello",2->"world")
    val maybeString: Option[String] = map.get(1)
    val value: Any = maybeString.getOrElse("ada")
    val someInt = Some(2)
    val value1: Int = someInt.get

    println(value1)
  }

}
