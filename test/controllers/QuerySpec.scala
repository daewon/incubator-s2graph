package controllers

import com.kakao.s2graph.core.utils.logger
import play.api.libs.json._
import play.api.test.{FakeApplication, FakeRequest, PlaySpecification}
import play.api.{Application => PlayApplication}

import scala.util.Random

class QuerySpec extends SpecCommon with PlaySpecification {

  import Helper._

  implicit val app = FakeApplication()

  init()

  "query test" should {
    running(FakeApplication()) {

      // insert bulk and wait ..
      val bulkEdges: String = Seq(
        edge"1000 insert e 0 1 $testLabelName"($(weight = 40, is_hidden = true)),
        edge"2000 insert e 0 2 $testLabelName"($(weight = 30, is_hidden = false)),
        edge"3000 insert e 2 0 $testLabelName"($(weight = 20)),
        edge"4000 insert e 2 1 $testLabelName"($(weight = 10)),
        edge"3000 insert e 10 20 $testLabelName"($(weight = 20)),
        edge"4000 insert e 20 20 $testLabelName"($(weight = 10)),
        edge"1 insert e -1 1000 $testLabelName",
        edge"1 insert e -1 2000 $testLabelName",
        edge"1 insert e -1 3000 $testLabelName",
        edge"1 insert e 1000 10000 $testLabelName",
        edge"1 insert e 1000 11000 $testLabelName",
        edge"1 insert e 2000 11000 $testLabelName",
        edge"1 insert e 2000 12000 $testLabelName",
        edge"1 insert e 3000 12000 $testLabelName",
        edge"1 insert e 3000 13000 $testLabelName",
        edge"1 insert e 10000 100000 $testLabelName",
        edge"2 insert e 11000 200000 $testLabelName",
        edge"3 insert e 12000 300000 $testLabelName").mkString("\n")

      val jsResult = contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true))
    }

    def queryParents(id: Long) = Json.parse(s"""
        {
          "returnTree": true,
          "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": 0,
              "limit": 2
            }
          ],[{
              "label": "${testLabelName}",
              "direction": "in",
              "offset": 0,
              "limit": -1
            }
          ]]
        }""".stripMargin)

    def queryExclude(id: Int) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": 0,
              "limit": 2
            },
            {
              "label": "${testLabelName}",
              "direction": "in",
              "offset": 0,
              "limit": 2,
              "exclude": true
            }
          ]]
        }""")

    def queryTransform(id: Int, transforms: String) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": 0,
              "transform": $transforms
            }
          ]]
        }""")

    def queryWhere(id: Int, where: String) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": 0,
              "limit": 100,
              "where": "${where}"
            }
          ]]
        }""")

    def querySingleWithTo(id: Int, offset: Int = 0, limit: Int = 100, to: Int) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": $offset,
              "limit": $limit,
              "_to": $to
            }
          ]]
        }
        """)

    def querySingle(id: Int, offset: Int = 0, limit: Int = 100) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelName}",
              "direction": "out",
              "offset": $offset,
              "limit": $limit
            }
          ]]
        }
        """)

    def queryUnion(id: Int, size: Int) = JsArray(List.tabulate(size)(_ => querySingle(id)))

    def queryGroupBy(id: Int, props: Seq[String], label: String = testLabelName, duplicate: String = "first"): JsValue = {
      Json.obj(
        "groupBy" -> props,
        "srcVertices" -> Json.arr(
          Json.obj("serviceName" -> testServiceName, "columnName" -> testColumnName, "id" -> id)
        ),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj(
                "limit" -> -1,
                "duplicate" -> duplicate,
                "label" -> label
              )
            )
          )
        )
      )
    }

    def queryScore(id: Int, scoring: Map[String, Int]): JsValue = {
      val q = Json.obj(
        "srcVertices" -> Json.arr(
          Json.obj(
            "serviceName" -> testServiceName,
            "columnName" -> testColumnName,
            "id" -> id
          )
        ),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj(
                "label" -> testLabelName,
                "scoring" -> scoring
              )
            )
          )
        )
      )
//      println(q)
      q
    }

    def queryOrderBy(id: Int, scoring: Map[String, Int], props: Seq[Map[String, String]]): JsValue = {
      Json.obj(
        "orderBy" -> props,
        "srcVertices" -> Json.arr(
          Json.obj("serviceName" -> testServiceName, "columnName" -> testColumnName, "id" -> id)
        ),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj(
                "label" -> testLabelName,
                "scoring" -> scoring
              )
            )
          )
        )
      )
    }

    def getEdges(queryJson: JsValue): JsValue = {
      val ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
      contentAsJson(ret)
    }

    def queryIndex(ids: Seq[Int], indexName: String) = {
      val $from = $a(
        $(serviceName = testServiceName,
          columnName = testColumnName,
          ids = ids))

      val $step = $a($(label = testLabelName, index = indexName))
      val $steps = $a($(step = $step))

      val js = $(withScore = false, srcVertices = $from, steps = $steps).toJson
      js
    }

    def queryDuration(ids: Seq[Int], from: Int, to: Int) = {
      val $from = $a(
        $(serviceName = testServiceName,
          columnName = testColumnName,
          ids = ids))

      val $step = $a($(
        label = testLabelName, direction = "out", offset = 0, limit = 100,
        duration = $(from = from, to = to)))

      val $steps = $a($(step = $step))

      $(srcVertices = $from, steps = $steps).toJson
    }

    "union query" in {
      running(FakeApplication()) {
        var result = getEdges(queryUnion(0, 2))
        result.as[List[JsValue]].size must equalTo(2)

        result = getEdges(queryUnion(0, 3))
        result.as[List[JsValue]].size must equalTo(3)

        result = getEdges(queryUnion(0, 4))
        result.as[List[JsValue]].size must equalTo(4)

        result = getEdges(queryUnion(0, 5))
        result.as[List[JsValue]].size must equalTo(5)

        val union = result.as[List[JsValue]].head
        val single = getEdges(querySingle(0))

        (union \\ "from").map(_.toString).sorted must equalTo((single \\ "from").map(_.toString).sorted)
        (union \\ "to").map(_.toString).sorted must equalTo((single \\ "to").map(_.toString).sorted)
        (union \\ "weight").map(_.toString).sorted must equalTo((single \\ "weight").map(_.toString).sorted)
      }
    }

    "get edge with where condition" in {
      running(FakeApplication()) {
        var result = getEdges(queryWhere(0, "is_hidden=false and _from in (-1, 0)"))
        (result \ "results").as[List[JsValue]].size must equalTo(1)

        result = getEdges(queryWhere(0, "is_hidden=true and _to in (1)"))
        (result \ "results").as[List[JsValue]].size must equalTo(1)

        result = getEdges(queryWhere(0, "_from=0"))
        (result \ "results").as[List[JsValue]].size must equalTo(2)

        result = getEdges(queryWhere(2, "_from=2 or weight in (-1)"))
        (result \ "results").as[List[JsValue]].size must equalTo(2)

        result = getEdges(queryWhere(2, "_from=2 and weight in (10, 20)"))
        (result \ "results").as[List[JsValue]].size must equalTo(2)
      }
    }

    "get edge exclude" in {
      running(FakeApplication()) {
        val result = getEdges(queryExclude(0))
        (result \ "results").as[List[JsValue]].size must equalTo(1)
      }
    }

    "get edge groupBy property" in {
      running(FakeApplication()) {
        val result = getEdges(queryGroupBy(0, Seq("weight")))
        (result \ "size").as[Int] must_== 2
        val weights = (result \\ "groupBy").map { js =>
          (js \ "weight").as[Int]
        }
        weights must contain(exactly(30, 40))
        weights must not contain(10)
      }
    }

    "groupBy ordering must be not changed" in {
      running(FakeApplication()) {
        val bulkEdges: String = Seq(
          edge"1001 insert e 1000 1 $testLabelNameWeak"($(time = 1001, weight = 10)),
          edge"1002 insert e 1000 2 $testLabelNameWeak"($(time = 1002, weight = 20)),
          edge"1003 insert e 1000 3 $testLabelNameWeak"($(time = 1003, weight = 10)),
          edge"1004 insert e 1000 1 $testLabelNameWeak"($(time = 1004, weight = 20)),
          edge"1005 insert e 1000 2 $testLabelNameWeak"($(time = 1005, weight = 10)),
          edge"1006 insert e 1000 3 $testLabelNameWeak"($(time = 1006, weight = 20))
        ).mkString("\n")

        contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true)).as[Seq[Boolean]] foreach { ret =>
          ret must_== true
        }
        val edges = getEdges(queryGroupBy(1000, Seq("to"), testLabelNameWeak, "raw"))
        logger.debug(s"$edges")
        val result = (edges \\ "groupBy").head

        (0 until 10) foreach { i =>
          val start = 5000 + 100 * i
          (0 until 10) foreach { j =>
            val ids = Random.shuffle(Seq("0", "1", "2", "3"))
            val it = ids.iterator

            val ts = start + j * 10
            val bulkEdges = Random.shuffle((0 until 4).toList).map { t =>
              edge"${ts + t} insert e 1000 ${it.next()} $testLabelNameWeak"($(time = ts + t, weight = 10))
            }.mkString("\n")
//            val bulkEdges: String = Seq(
//              edge"${j + 3} insert e 1000 1 $testLabelNameWeak"($(time = j + 3, weight = 10)),
//              edge"${j + 2} insert e 1000 2 $testLabelNameWeak"($(time = j + 2, weight = 10)),
//              edge"${j + 1} insert e 1000 3 $testLabelNameWeak"($(time = j + 1, weight = 20)),
//              edge"$j insert e 1000 0 $testLabelNameWeak"($(time = j, weight = 0))
//            ).mkString("\n")

            contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true)).as[Seq[Boolean]] foreach { ret =>
              ret must_== true
            }
            val edges = getEdges(queryGroupBy(1000, Seq("to"), testLabelNameWeak, "raw"))
//            logger.error(s"$edges")
            (edges \\ "groupBy").head must_== result
          }
        }

        (edges \\ "groupBy").head must_== result
      }
    }

    "edge transform " in {
      running(FakeApplication()) {
        var result = getEdges(queryTransform(0, "[[\"_to\"]]"))
        (result \ "results").as[List[JsValue]].size must equalTo(2)

        result = getEdges(queryTransform(0, "[[\"weight\"]]"))
        (result \\ "to").map(_.toString).sorted must equalTo((result \\ "weight").map(_.toString).sorted)

        result = getEdges(queryTransform(0, "[[\"_from\"]]"))
        val results = (result \ "results").as[JsValue]
        (result \\ "to").map(_.toString).sorted must equalTo((results \\ "from").map(_.toString).sorted)
      }
    }

    "index" in {
      running(FakeApplication()) {
        // weight order
        var result = getEdges(queryIndex(Seq(0), "idx_1"))
        ((result \ "results").as[List[JsValue]].head \\ "weight").head must equalTo(JsNumber(40))

        // timestamp order
        result = getEdges(queryIndex(Seq(0), "idx_2"))
        ((result \ "results").as[List[JsValue]].head \\ "weight").head must equalTo(JsNumber(30))
      }
    }

    "checkEdges" in {
      running(FakeApplication()) {
        val json = Json.parse(s"""
         [{"from": 0, "to": 1, "label": "$testLabelName"},
          {"from": 0, "to": 2, "label": "$testLabelName"}]
        """)

        def checkEdges(queryJson: JsValue): JsValue = {
          val ret = route(FakeRequest(POST, "/graphs/checkEdges").withJsonBody(queryJson)).get
          contentAsJson(ret)
        }

        val res = checkEdges(json)
        val typeRes = res.isInstanceOf[JsArray]
        typeRes must equalTo(true)

        val fst = res.as[Seq[JsValue]].head \ "to"
        fst.as[Int] must equalTo(1)

        val snd = res.as[Seq[JsValue]].last \ "to"
        snd.as[Int] must equalTo(2)
      }
    }

    "duration" in {
      running(FakeApplication()) {
        // get all
        var result = getEdges(queryDuration(Seq(0, 2), from = 0, to = 5000))
        (result \ "results").as[List[JsValue]].size must equalTo(4)

        // inclusive, exclusive
        result = getEdges(queryDuration(Seq(0, 2), from = 1000, to = 4000))
        (result \ "results").as[List[JsValue]].size must equalTo(3)

        result = getEdges(queryDuration(Seq(0, 2), from = 1000, to = 2000))
        (result \ "results").as[List[JsValue]].size must equalTo(1)

        val bulkEdges: String = Seq(
          edge"1001 insert e 0 1 $testLabelName"($(weight = 10, is_hidden = true)),
          edge"2002 insert e 0 2 $testLabelName"($(weight = 20, is_hidden = false)),
          edge"3003 insert e 2 0 $testLabelName"($(weight = 30)),
          edge"4004 insert e 2 1 $testLabelName"($(weight = 40))
        ).mkString("\n")

        val jsResult = contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true))
        // duration test after udpate
        // get all
        result = getEdges(queryDuration(Seq(0, 2), from = 0, to = 5000))
        (result \ "results").as[List[JsValue]].size must equalTo(4)

        // inclusive, exclusive
        result = getEdges(queryDuration(Seq(0, 2), from = 1000, to = 4000))
        (result \ "results").as[List[JsValue]].size must equalTo(3)

        result = getEdges(queryDuration(Seq(0, 2), from = 1000, to = 2000))
        (result \ "results").as[List[JsValue]].size must equalTo(1)
        true
      }
    }

    "returnTree" in {
      running(FakeApplication()) {
        val src = 100
        val tgt = 200
        val labelName = testLabelName

        val bulkEdges: String = Seq(
          edge"1001 insert e $src $tgt $labelName"
        ).mkString("\n")

        val jsResult = contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true))

        val result = getEdges(queryParents(src))

        val parents = (result \ "results").as[Seq[JsValue]]
        val ret = parents.forall { edge => (edge \ "parents").as[Seq[JsValue]].size == 1 }
        ret must equalTo(true)
      }
    }

    "pagination and _to" in {
      running(FakeApplication()) {
        val src = System.currentTimeMillis().toInt
        val labelName = testLabelName
        val bulkEdges: String = Seq(
          edge"1001 insert e $src 1 $labelName"($(weight = 10, is_hidden = true)),
          edge"2002 insert e $src 2 $labelName"($(weight = 20, is_hidden = false)),
          edge"3003 insert e $src 3 $labelName"($(weight = 30)),
          edge"4004 insert e $src 4 $labelName"($(weight = 40))
        ).mkString("\n")

        val jsResult = contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true))

        var result = getEdges(querySingle(src, offset = 0, limit = 2))
//        println(result)
        var edges = (result \ "results").as[List[JsValue]]
        edges.size must equalTo(2)
        (edges(0) \ "to").as[Long] must beEqualTo(4)
        (edges(1) \ "to").as[Long] must beEqualTo(3)

        result = getEdges(querySingle(src, offset = 1, limit = 2))
//        println(result)
        edges = (result \ "results").as[List[JsValue]]
        edges.size must equalTo(2)
        (edges(0) \ "to").as[Long] must beEqualTo(3)
        (edges(1) \ "to").as[Long] must beEqualTo(2)

        result = getEdges(querySingleWithTo(src, offset = 0, limit = -1, to = 1))
//        println(result)
        edges = (result \ "results").as[List[JsValue]]
        edges.size must equalTo(1)
      }
    }

    "orderBy" >> {
      running(FakeApplication()) {
        // insert test set
        val bulkEdges: String = Seq(
          edge"1001 insert e 0 1 $testLabelName"($(weight = 10, is_hidden = true)),
          edge"2002 insert e 0 2 $testLabelName"($(weight = 20, is_hidden = false)),
          edge"3003 insert e 2 0 $testLabelName"($(weight = 30)),
          edge"4004 insert e 2 1 $testLabelName"($(weight = 40))
        ).mkString("\n")
        contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true))

        // get edges
        val edges = getEdges(queryScore(0, Map("weight" -> 1)))
        val orderByScore = getEdges(queryOrderBy(0, Map("weight" -> 1), Seq(Map("score" -> "DESC", "timestamp" -> "DESC"))))
        val ascOrderByScore = getEdges(queryOrderBy(0, Map("weight" -> 1), Seq(Map("score" -> "ASC", "timestamp" -> "DESC"))))

//        println(edges)
//        println(orderByScore)
//        println(ascOrderByScore)

        val edgesTo = edges \ "results" \\ "to"
        val orderByTo = orderByScore \ "results" \\ "to"
        val ascOrderByTo = ascOrderByScore \ "results" \\ "to"

        edgesTo must_== Seq(JsNumber(2), JsNumber(1))
        edgesTo must_== orderByTo
        ascOrderByTo must_== Seq(JsNumber(1), JsNumber(2))
        edgesTo.reverse must_== ascOrderByTo
      }
    }
  }
}
