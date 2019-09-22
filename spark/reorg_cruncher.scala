def reorg(datadir :String)
{
  val t0 = System.nanoTime()

  val person   = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
    load(datadir + "/person.*csv.*")
  val interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
    load(datadir + "/interest.*csv.*")
  val knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
    load(datadir + "/knows.*csv.*")

  val a=0
  val know_each=knows.join(knows.select($"personId".alias("friendId"),$"friendId".alias("f")),"friendId").filter($"f"===$"personId")
  val loc_know=know_each.join(person.select($"personId",$"locatedIn".alias("ploc"),$"birthday"),"personId").join(person.select($"personId".alias("friendId"),$"locatedIn".alias("floc")),"friendId").filter($"ploc"===$"floc").cache
  val new_person=loc_know.select($"personId",$"birthday").distinct.cache

  for(a <- 1 until 13)
  {
  new_person.filter(month($"birthday") ===a).write.parquet(datadir+"/new_person_"+a)
  println("PERSON " + a)
  }
  val new_know=loc_know.select($"personId",$"friendId").distinct
  new_know.write.parquet(datadir+"/new_knows_whole")
  val interest_loc=loc_know.join(interest,"personId")

  val new_interes=interest_loc.select($"personId",$"interest").distinct.cache

  for(a <- 0 until 20)
  {
  new_interes.filter($"interest"%20===a).write.parquet(datadir+"/new_interest/new_interest_"+a)
  println("INTEREST " + a)
  }

  val t1 = System.nanoTime()
  println("reorg time: " + (t1 - t0)/1000000 + "ms")
}

def cruncher(datadir :String, a1 :Int, a2 :Int, a3 :Int, a4 :Int, lo :Int, hi :Int) :org.apache.spark.sql.DataFrame =
{
  val t0 = System.nanoTime()

  val begin=lo/100
  val end=hi/100
  var person   = spark.read.parquet(datadir + "/new_person_"+end)
  val i=0;
  println("begin: " + lo+" _"+begin)
  println("end:  " + hi+" _"+end)
  for(i <- begin until end)
    {
      person=person.union(spark.read.parquet(datadir + "/new_person_"+i))
      println("MONTH " + i)
    }
  val person_df=person.toDF
  val person_birth=person_df.
    withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday")).
    filter($"bday" >= lo && $"bday" <= hi)

  val A1 = spark.read.parquet(datadir + "/new_interest/new_interest_"+a1%20).filter($"interest"===a1)
  val A2 = spark.read.parquet(datadir + "/new_interest/new_interest_"+a2%20).filter($"interest"===a2)
  val A3 = spark.read.parquet(datadir + "/new_interest/new_interest_"+a3%20).filter($"interest"===a3)
  val A4 = spark.read.parquet(datadir + "/new_interest/new_interest_"+a4%20).filter($"interest"===a4)
  val score = A2.union(A3).union(A4).union(A1).toDF.withColumn("nofun",$"interest".notEqual(a1)).groupBy("personId").agg(count("personId") as "score",min("nofun") as "nofun").filter($"score">0 && $"nofun")
//  val score=A2.union(A3).union(A4).toDF.groupBy("personId").agg(count("personId") as "score").filter($"score">0)
  val score = score_initial.join(person_birth,"personId").filter($"interest".notEqual(a1))
  val fun_a1_person=A1.join(person_birth,"personId")
  val knows    = spark.read.parquet(datadir + "/new_knows_whole")

  val results=fun_a1_person.join(knows,"personId").join(score.select($"personId".alias("friendId"),$"score"),"friendId").select($"personId".alias("f"), $"friendId".alias("p"), $"score")

  val ret      = results.select($"p", $"f", $"score").orderBy(desc("score"), asc("p"), asc("f"))

  ret.show(1000) // force execution now, and display results to stdout

  val t1 = System.nanoTime()
  println("cruncher time: " + (t1 - t0)/1000000 + "ms")

  return ret
}
