/**
  * Created by yang on 5/20/16.
  */
object RegExpTest extends App {

  val source = "http:\\/\\/weibo.com\\/u\\/1937547281?refer_flag=2307710005_5902256596\\"
  val pattern = "(?<=http:\\\\/\\\\/weibo.com\\\\/)(u\\\\/)?\\w+(?=\\?refer_flag)".r

  val result = pattern.findAllIn(source)

  while (result.hasNext) {
    println(result.next().replaceAll("\\\\", ""))
  }
}
