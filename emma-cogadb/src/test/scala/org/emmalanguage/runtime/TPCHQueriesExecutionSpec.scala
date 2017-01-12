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

import compiler.lang.cogadb.ast
import test.util._

import org.scalatest._

import java.io.File
import java.nio.file.Paths

class TPCHQueriesExecutionSpec extends FreeSpec with Matchers with BeforeAndAfter {

  val dir = "/cogadb"
  val path = tempPath("/cogadb")

  var cogadb: CoGaDB = null

  before {
    new File(path).mkdirs()
    val coGaDBPath = Paths.get(System.getProperty("coGaDBPath", "cogadb"))
    val configPath = Paths.get(materializeResource(s"$dir/tpch.coga"))

    cogadb = CoGaDB(coGaDBPath, configPath)
    //println(configPath)
  }

  after {
    //deleteRecursive(new File(path))
    //cogadb.destroy()
  }

  "read CUSTOMER" in {

    val customerScan = ast.TableScan("CUSTOMER")

    cogadb.execute(customerScan)
  }

  "sort CUSTOMER" in {
    val sortBy = ast.Sort(Seq(
      ast.SortCol("CUSTOMER","C_CUSTKEY","INT","C_CUSTKEY",1,"ASCENDING")), ast.TableScan("CUSTOMER"))
    cogadb.execute(sortBy)

  }

  "filter CUSTOMER" in {

    val customerSelection = ast.Selection(Seq(ast.And(Seq(
                              ast.ColConst(
                                ast.AttrRef("CUSTOMER", "CUSTKEY", "CUSTKEY", 1),
                                ast.IntConst(500),
                                ast.LessEqual)
                              ))),
                              ast.TableScan("CUSTOMER")
                              )

    cogadb.execute(customerSelection)
  }


  "create A" in {
  val schemaForA = Seq(ast.SchemaAttr("INT","id"),ast.SchemaAttr("VARCHAR", "name"))

  val readA = ast.ImportFromCsv("A", "/home/haros/Desktop/emmaToCoGaDB/sample_tables/A.csv", ",", schemaForA)


    cogadb.execute(readA)
    //cogadb.executeGeneral("import_csv_file","dataflow0000 /home/haros/Desktop/emmaToCoGaDB/sample_tables/A.csv")

  }

  "write" in {
    val scan = cogadb.write(Seq(), 'schema)

    //scan shouldBe

  }

  "join A and B" in {
    val A = Seq((1, "foo"), (2, "bar"))
    val B = Seq((1, 42.0), (2, 30.0))

    val scanA = cogadb.write(A, 'schemaForA)
    val scanB = cogadb.write(B, 'schemaForB)

    val joinR = cogadb.execute(ast.Join(???, ???, scanA, scanB))

    val res = cogadb.read[(Int, String, Double)](joinR)
    val exp = Seq((1, "foo", 42.0), (2, "bar", 30.0))

    res shouldBe exp
  }
}
