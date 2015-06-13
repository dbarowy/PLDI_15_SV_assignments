import java.io.File
import java.text.SimpleDateFormat
import java.util.TimeZone
import com.github.tototoshi.csv._
import org.joda.time.{DateTimeZone, LocalDateTime}

object PLDIAssignments extends App {
  if (args.size == 0) {
    println("Usage:")
    println("\tsbt \"run <users.csv> <assignments.csv> <print times? true/false>\"")
    sys.exit(1)
  }

  val csv_users = args(0)
  val csv_assignments = args(1)
  val with_times = if (args.size == 2) { false } else { args(2).toBoolean }

  val reader_users = CSVReader.open(new File(csv_users))
  val users: List[Map[String, String]] = reader_users.allWithHeaders()
  reader_users.close()

  val user_db = users.map { row =>
    val vol_id = row("id")
    val last = row("last")
    val first = row("first")
    val email = row("email")
    val gender = row("gender")
    vol_id -> Map('first -> first, 'last -> last, 'gender -> gender, 'email -> email)
  }.toMap

  val reader_assignments = CSVReader.open(new File(csv_assignments))
  val assignments: List[Map[String, String]] = reader_assignments.allWithHeaders()
  reader_assignments.close()

  // define date format parser
  val df = new SimpleDateFormat("dd.MM.yyyy kk:mm zzzz")

  // for each user, find all of that user's assignments
  // want:
  // user -> List((assignment1,time1),...,(assignmentn,timen))
  val user_assignments = users.flatMap { row =>
    val vol_id = row("id")

    val assns = assignments.flatMap { arow =>
      val volunteers = arow("assigned_volunteers").split(";")
      if(volunteers.contains(vol_id)) {
        // parse dates & convert to PDT
        val start_d = date_conv(df.parse(arow("start")))
        val end_d = date_conv(df.parse(arow("end")))

        // unfortunately, the string representation is still wrong
        val start_conv = start_d.toString.replace("EDT","PDT")
        val end_conv = end_d.toString.replace("EDT","PDT")

        // compute duration in mins
        val duration = (end_d.getTime - start_d.getTime)/1000/60

        Some((arow("event"),start_conv,end_conv,duration,arow("total_duration"),start_d,end_d))
      } else {
        None
      }
    }

    if (assns.isEmpty) {
      None
    } else {
      Some(vol_id -> assns)
    }
  }

  user_assignments.foreach { case (vol_id, assns) =>
    val total_duration = assns.foldLeft(0L){ case (acc,(_,_,_,_,admin_duration,_,_)) => acc + admin_duration.toInt}

    val duration_txt = if (with_times) { " (" + total_duration + " total minutes)" } else { "" }

    println(user_db(vol_id)('first) +
            " " + user_db(vol_id)('last) +
            " <" + user_db(vol_id)('email) + ">" +
            duration_txt
    )
    assns.foreach { case(event, start, end, duration, admin_duration, start_d, end_d) =>
      println("\t" + start + " UNTIL " + end + " (" + admin_duration + " minutes)" + "\t" + event)
    }

    println()
  }

  val hourly_schedule = user_assignments.map { case (volunteer_id, assignments) =>
    assignments.map { case (event, start, end, duration, admin_duration, start_d, end_d) =>

      // unfortunately, the string representation is still wrong
      val start_conv = start_d.toString.replace("EDT","PDT")
      val end_conv = end_d.toString.replace("EDT","PDT")
      (event, start_conv.toString, end_conv.toString, duration, admin_duration, volunteer_id)
    }
  }.flatten.sortBy { case (event, start, end, duration, admin_duration, volunteer_id) => start }

  hourly_schedule.foreach { case (event, start, end, duration, admin_duration, volunteer_id) =>
    println(List(start, end, event, admin_duration, (user_db(volunteer_id)('first) + " " + user_db(volunteer_id)('last))).mkString(", "))
  }

//  assignments.sortBy{ arow => arow("start") }.foreach { arow =>
//    val event = arow("event")
//    val start_d = date_conv(df.parse(arow("start")))
//    val end_d = date_conv(df.parse(arow("end")))
//    val duration = (end_d.getTime - start_d.getTime)/1000/60
//    println(List(event,arow("start"),arow("end"),duration).mkString(","))
//  }

  def date_conv(date: java.util.Date) : java.util.Date = {
    val start_dj = new LocalDateTime(date)
    val srcDateTime = start_dj.toDateTime(DateTimeZone.forID("US/Eastern"))
    val dstDateTime = srcDateTime.withZone(DateTimeZone.forID("US/Pacific"))
    dstDateTime.toLocalDateTime.toDateTime.toDate
  }
}
