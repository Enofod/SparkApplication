package com.dkunert.mgr.util

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.poi.xssf.usermodel.XSSFWorkbook

object ExcelUtil {



  def writeToExcel(file: String, rowNumber: Integer, list: List[Double]): Unit = {

    var workbook = new XSSFWorkbook()

    val existingExcelFile = new File(file)
    if (existingExcelFile.exists()) {
      workbook = new XSSFWorkbook(new FileInputStream(existingExcelFile))
    } else {
      if (workbook.getNumberOfSheets() == 0) {
        workbook.createSheet("metrics")
      }

      var sheet = workbook.getSheetAt(0)
      var row = sheet.createRow(0)
      row.createCell(0).setCellValue("Iteracja")
      row.createCell(1).setCellValue("Czas uczenia")
      row.createCell(2).setCellValue("Czas predykcji")
      row.createCell(3).setCellValue("Prezyzja")
      row.createCell(4).setCellValue("Powierzchnia pod ROC")
      row.createCell(5).setCellValue("Powierzchnia pod PR")
    }

    var sheet = workbook.getSheetAt(0)

    var row = sheet.createRow(rowNumber)

    var counter = 0
    list.foreach(item => {
      row.createCell(counter).setCellValue(item)
      counter += 1
    })

    // Write the output to a file// Write the output to a file
    val fileOut: FileOutputStream = new FileOutputStream(existingExcelFile)
    workbook.write(fileOut)
    fileOut.close()

    // Closing the workbook
    workbook.close()

  }
}
