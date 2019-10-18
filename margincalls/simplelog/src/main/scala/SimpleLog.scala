import java.text.SimpleDateFormat
import java.util.Date

/** Provides a simple log utility that registers the execution times for the
 *  steps of an application and print them in console.
 *
 *  @param appName the name of the application
 */
class SimpleLog(appName: String) {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  // Prints the time when the application begins
  def beginProgram = {
    val date = dateFormat.format(new Date)

    println("-"*80)
    println(s"$date BEGIN PROGRAM $appName")
    println("-"*80)
  }

 // Prints the time when an application step begins
  def writeMsg(msg: String) = {
    val date = dateFormat.format(new Date)

    println(s"$date ${appName}: $msg")
  }

  // Prints the time when the application ends
  def endProgram = {
    val date = dateFormat.format(new Date)

    println("-"*80)
    println(s"$date END OF PROGRAM $appName")
    println("-"*80)
  }
}
