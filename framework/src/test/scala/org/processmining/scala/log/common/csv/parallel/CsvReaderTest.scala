package org.processmining.scala.log.common.csv.parallel

import java.util.regex.Pattern

import org.scalatest.FunSuite

class CsvReaderTest extends FunSuite {

//  //TODO: remove after fixing
//  test("testReadBpi2017Apps") {
//    val reader = new CsvReader(CsvReader.getCsvLineSplitterRegex(","), "\"")
//    val (header, lines) = reader.read("D:\\Users\\nlvden\\Documents\\BPI\\BPI2017\\apps\\apps.csv")
//    val columnsInHeader = header.length
//    lines.foreach(x => {
//      //      println(println(x.mkString(",")))
//      //      println(println(s"l=${x.length}"))
//      assert(x.length == columnsInHeader)
//    }
//    )
//  }

  test("testsplitterImpl") {
    val columns = 18
    val header = """"case","event","startTime","completeTime","LoanGoal","ApplicationType","RequestedAmount","Action","FirstWithdrawalAmount","NumberOfTerms","Accepted","OfferID","MonthlyCost","EventOrigin","EventID","Selected","CreditScore","OfferedAmount""""
    val idealString = """"0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17""""
    //val notIdealString1 = """"Application_652823628","A_Create Application","2016/01/01 10:51:15.304","COMPLETE","New credit","Existing loan takeover","20000.0",,"User_1",,"Application_652823628",,,"Created",,,,"Application","""
    val notIdealString2 = """"Application_652823628","A_Create Application","2016/01/01 10:51:15.304","2016/01/01 10:51:15.304","Existing loan takeover","New credit","20000.0","Created",,,,,,"Application","Application_652823628",,,"""
    val notIdealString3 = """"Application_1350494635","W_Complete application","2017/01/02 20:15:15.214","2017/01/02 20:15:15.214","Home improvement","New credit","20000.0","Obtained",,,,,,"Workflow","Workitem_1385765706",,,"""
    val notIdealString2NoWrapper = "Application_652823628,A_Create Application,2016/01/01 10:51:15.304,2016/01/01 10:51:15.304,Existing loan takeover,New credit,20000.0,Created,,,,,,Application,Application_652823628,,,"
    val splitter = CsvReader.splitterImplV2(Pattern.compile(CsvReader.getCsvLineSplitterRegex(",")), "\"", _: String)
    assert(header.split(",").length == columns)
    assert(splitter(header).length == columns)
    assert(splitter(idealString).length == columns)
    //assert(splitter(notIdealString1).length == columns)
    assert(splitter(notIdealString2).length == columns)
    assert(splitter(notIdealString3).length == columns)
    assert(splitter(notIdealString2NoWrapper).length == columns)

    assert(splitter(notIdealString2)(0) == "Application_652823628")
    assert(splitter(notIdealString2)(1) == "A_Create Application")
    assert(splitter(notIdealString2)(2) == "2016/01/01 10:51:15.304")
    assert(splitter(notIdealString2)(3) == "2016/01/01 10:51:15.304")
    assert(splitter(notIdealString2)(4) == "Existing loan takeover")
    assert(splitter(notIdealString2)(5) == "New credit")
    assert(splitter(notIdealString2)(6) == "20000.0")
    assert(splitter(notIdealString2)(7) == "Created")
    assert(splitter(notIdealString2)(8) == "")
    assert(splitter(notIdealString2)(9) == "")
    assert(splitter(notIdealString2)(10) == "")
    assert(splitter(notIdealString2)(11) == "")
    assert(splitter(notIdealString2)(12) == "")
    assert(splitter(notIdealString2)(13) == "Application")
    assert(splitter(notIdealString2)(14) == "Application_652823628")
    assert(splitter(notIdealString2)(15) == "")
    assert(splitter(notIdealString2)(16) == "")
    assert(splitter(notIdealString2)(17) == "")

    assert(splitter(notIdealString2NoWrapper)(0) == "Application_652823628")
    assert(splitter(notIdealString2NoWrapper)(1) == "A_Create Application")
    assert(splitter(notIdealString2NoWrapper)(2) == "2016/01/01 10:51:15.304")
    assert(splitter(notIdealString2NoWrapper)(3) == "2016/01/01 10:51:15.304")
    assert(splitter(notIdealString2NoWrapper)(4) == "Existing loan takeover")
    assert(splitter(notIdealString2NoWrapper)(5) == "New credit")
    assert(splitter(notIdealString2NoWrapper)(6) == "20000.0")
    assert(splitter(notIdealString2NoWrapper)(7) == "Created")
    assert(splitter(notIdealString2NoWrapper)(8) == "")
    assert(splitter(notIdealString2NoWrapper)(9) == "")
    assert(splitter(notIdealString2NoWrapper)(10) == "")
    assert(splitter(notIdealString2NoWrapper)(11) == "")
    assert(splitter(notIdealString2NoWrapper)(12) == "")
    assert(splitter(notIdealString2NoWrapper)(13) == "Application")
    assert(splitter(notIdealString2NoWrapper)(14) == "Application_652823628")
    assert(splitter(notIdealString2NoWrapper)(15) == "")
    assert(splitter(notIdealString2NoWrapper)(16) == "")
    assert(splitter(notIdealString2NoWrapper)(17) == "")
  }
}
