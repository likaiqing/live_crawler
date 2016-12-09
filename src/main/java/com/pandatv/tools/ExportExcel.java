package com.pandatv.tools;

import com.pandatv.pojo.Book;
import com.pandatv.pojo.Student;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import javax.swing.*;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by likaiqing on 2016/12/8.
 */
public class ExportExcel<T> {
    public static final String FILE_SEPARATOR = System.getProperties()
            .getProperty("file.separator");

    public void exportExcel(String title, String[] headers, List<Map<String, String>> maps,
                            OutputStream out, String[] fields) {
        exportExcel(title, headers, maps, out, fields, "yyyy-MM-dd");
    }

    private void exportExcel(String title, String[] headers, List<Map<String, String>> maps, OutputStream out, String[] fieldsName, String pattern) {
        XSSFWorkbook workbook = new XSSFWorkbook();
        // 生成一个表格
        XSSFSheet sheet = workbook.createSheet(title);
        // 设置表格默认列宽度为15个字节
        sheet.setDefaultColumnWidth((short) 15);
        XSSFRow row = sheet.createRow(0);
        for (short i = 0; i < headers.length; i++) {
            XSSFCell cell = row.createCell(i);
//            cell.setCellStyle(style);
            cell.setCellValue(headers[i]);
        }
        int index=0;
        for (int i = 0; i < maps.size(); i++) {
            Map<String, String> fieldMap = maps.get(i);
            row = sheet.createRow(++index);
            for (int j = 0; j < fieldsName.length; j++) {
                XSSFCell cell = row.createCell(j);
                String fieldName = fieldsName[j];
                String value = fieldMap.get(fieldName);
                cell.setCellValue(value);
            }
        }
        try {
            workbook.write(out);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void test(String imagesPath, String docsPath) {
        // 测试学生
        ExportExcel<Student> ex = new ExportExcel<Student>();
        String[] headers = {"学号", "姓名", "年龄", "性别", "出生日期"};
        List<Student> dataset = new ArrayList<Student>();
        dataset.add(new Student(10000001, "张三", 20, true, new Date()));
        dataset.add(new Student(20000002, "李四", 24, false, new Date()));
        dataset.add(new Student(30000003, "王五", 22, true, new Date()));
        // 测试图书
        ExportExcel<Book> ex2 = new ExportExcel<Book>();
        String[] headers2 = {"图书编号", "图书名称", "图书作者", "图书价格", "图书ISBN",
                "图书出版社", "封面图片"};
        List<Book> dataset2 = new ArrayList<Book>();
        try {
            BufferedInputStream bis = new BufferedInputStream(
                    new FileInputStream(imagesPath + FILE_SEPARATOR
                            + "book.png"));
            byte[] buf = new byte[bis.available()];
            while ((bis.read(buf)) != -1) {
                //
            }
            dataset2.add(new Book(1, "jsp", "leno", 300.33f, "1234567",
                    "清华出版社", buf));
            dataset2.add(new Book(2, "java编程思想", "brucl", 300.33f, "1234567",
                    "阳光出版社", buf));
            dataset2.add(new Book(3, "DOM艺术", "lenotang", 300.33f, "1234567",
                    "清华出版社", buf));
            dataset2.add(new Book(4, "c++经典", "leno", 400.33f, "1234567",
                    "清华出版社", buf));
            dataset2.add(new Book(5, "c#入门", "leno", 300.33f, "1234567",
                    "汤春秀出版社", buf));
            OutputStream out = new FileOutputStream(docsPath + FILE_SEPARATOR
                    + "export2003_a.xls");
            OutputStream out2 = new FileOutputStream(docsPath + FILE_SEPARATOR
                    + "export2003_b.xls");
//            ex.exportExcel(headers, dataset, out);
//            ex2.exportExcel(headers2, dataset2, out2);
            out.close();
            out2.close();
            JOptionPane.showMessageDialog(null, "导出成功!");
            System.out.println("excel导出成功！");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
