/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package runtime

import api.DataBag
import compiler.lang.cogadb._
import io.csv._

import net.liftweb.json._

import sys.process._

import java.io._
import java.nio.file.Files
import java.nio.file.Path

/** CoGaDB runtime. */
class CoGaDB private(coGaDBPath: Path, configPath: Path) {

  val csv = CSV()

  val inst = Seq(coGaDBPath.toString, configPath.toString).run(true)
  val tempPath = Files.createTempDirectory("emma-cogadbd").toAbsolutePath

  var dflNames = Stream.iterate(0)(_ + 1).map(i => f"dataflow$i%04d").toIterator
  var srcNames = Stream.iterate(0)(_ + 1).map(i => f"source$i%04d").toIterator

  sys.addShutdownHook({
    inst.destroy()
    Files.delete(tempPath)
  })

  /**
   * Execute a the dataflow rooted at `df` and write the result in a fresh table in CoGaDB.
   *
   * @return A table scan over the newly created table.
   */
  def execute(df: ast.Op): ast.TableScan = df match {
    case df@ast.TableScan(_, _) =>
      df // just a table scan, nothing to do
    case df@ast.MaterializeResult(tableName, persistOnDisk, child) =>
      // already wrapped in a `MaterializeResult` by the client
      executeOnCoGaDB(df)
      ast.TableScan(tableName)
    case _ =>
      // default case
      val dflName = dflNames.next()
      executeOnCoGaDB(ast.MaterializeResult(dflName, false, df))
      ast.TableScan(dflName)
  }

  def write[A](seq: Seq[A], schemaType: Symbol)(implicit converter: CSVConverter[A]): ast.TableScan = {
    // write into csv
    val dstName = srcNames.next()
    val dstPath = tempPath.resolve(s"$dstName.csv")

    // TODO: construct and execute a dataflow that imports from the csv location



    // TODO: Save Seq to CSV (now hardcoded file)
    // TODO: Fix schema (now hardcoded)

    val tmpSchema = Seq(ast.SchemaAttr("INT","id"),ast.SchemaAttr("VARCHAR", "name"))

    val read = ast.MaterializeResult(dstName,false,
      ast.ImportFromCsv(dstName, "/home/haros/Desktop/emmaToCoGaDB/sample_tables/A.csv", ",", tmpSchema))



    execute(read)

    ast.TableScan(dstName)
  }

  val schemaTypes: Map[Symbol, String] = Map(
    'schemaForA -> "Schema for A definition",
    'schemaForB -> "Schema for B"
  )

  def read[A](df: ast.TableScan)(implicit converter: CSVConverter[A]): Seq[A] = {
    // write into csv
    val srcName = df.tableName
    val srcPath = tempPath.resolve(s"$srcName.csv")

    // TODO: construct and execute a dataflow that exports from the table location

    val export = ast.ExportToCsv(srcPath.toString, ",", df)
    execute(export)

    DataBag.readCSV[A](srcPath.toString, csv).fetch()
  }

  def destroy(): Unit =
    inst.destroy()

  private def saveJson(df: ast.MaterializeResult): Path = {
    val path = tempPath.resolve(s"${df.tableName}.json")
    /*for {
      fw <- new FileWriter(path.toFile)
      bw <- new BufferedWriter(fw)
    } yield {
      //wrap in ast.Root
      try {
        bw.write(prettyRender(asJson(ast.Root(df))))
      }      catch {
        case ioe: IOException => println(ioe)
      }
    }*/
    val file = new File(path.toString)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(prettyRender(asJson(ast.Root(df))))
    bw.close()
    path
  }

  private def executeOnCoGaDB(df: ast.MaterializeResult) = {
    try {


      val path = saveJson(df)
      val output = (s"echo execute_query_from_json $path" #| "nc localhost 8000").!!
      println(output)
    } catch {
      case e: Exception => println("Warning: no process listening on port 8000")
    }
  }
  def executeGeneral(cmd: String, params: String) = {
    try {


      val output = (s"$cmd $params" #| "nc localhost 8000").!!
      println(output)
    } catch {
      case e: Exception => println("Warning: no process listening on port 8000")
    }
  }
}


object CoGaDB {

  def apply(coGaDBPath: Path, configPath: Path): CoGaDB = {
    new CoGaDB(coGaDBPath.resolve("bin/cogadbd"), configPath)}

  def main(args: Array[String]): Unit = {

   /* val dir = "/cogadb"
    val path = tempPath("/cogadb")

    var cogadb: CoGaDB = null


      new File(path).mkdirs()
      val coGaDBPath = Paths.get(System.getProperty("coGaDBPath", "cogadb"))
      val configPath = Paths.get(materializeResource(s"$dir/tpch.coga"))

      cogadb = CoGaDB(coGaDBPath, configPath)*/
    /*val customerScan = ast.Root(ast.TableScan("CUSTOMER"))
    val location = "/home/haros/Desktop/emmaToCoGaDB/generated/"
    val jsonPath = saveJson(customerScan,location)
    executeOnCoGaDB(jsonPath)*/

  }
}