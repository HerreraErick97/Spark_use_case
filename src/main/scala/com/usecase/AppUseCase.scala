package com.usecase

import com.usecase.sql.Transformation
import com.usecase.utils.AppSession

object AppUseCase extends AppSession {

  def run(): Unit = {

    /**
     * INPUTS
     */
    logger.info("=====> Reading file")

    val dfAthletes = readParquet("input.pathAthletes")
    val dfCoaches = readParquet("input.pathCoaches")
    val dfMedals = readParquet("input.pathMedals")


    /**
     * TRANSFORMATIONS
     */
    logger.info("=====> Transforming data")
    //val dfAthletesWithExtraColumn = Transformation.addLiteralColumn(dfAthletes)
    val topAthletes = Transformation.createTop(dfAthletes)
    val topCoaches = Transformation.createTop(dfCoaches)


    /**
     * FILTER
     */
    val athletesFiltered = Transformation.getTableFiltered(topAthletes)
    val coachesFiltered = Transformation.getTableFiltered(topCoaches)
    val medalsFiltered = Transformation.getTableFiltered(dfMedals)

    /**
     * PERCENT
     */
    val athletesResume = Transformation.createPercent(athletesFiltered)
    val coachesResume = Transformation.createPercent(coachesFiltered)


    /**
     * MESSAGE
     */
    val dfMessage = Transformation.createMessage(medalsFiltered)

    /**
     * JOIN
     */
    val dfJoin = Transformation.joinObjects(athletesResume, coachesResume, dfMessage )

    /**
     * OUTPUT
     */
    //createTop
    //topAthletes.show()
    //topCoaches.show()

    //TableFiltered
    //athletesFiltered.show()
    //medalsFiltered.show()

    //tableResume
    //athletesResume.show()

    //message
    //dfMessage.show(false)

    //join
    //dfJoin.show(false)

    writeParquet(dfJoin, "output.path")
    spark.stop()
    logger.info("=====> end process")

  }
}
