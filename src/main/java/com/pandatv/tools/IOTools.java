package com.pandatv.tools;

import com.pandatv.pojo.DetailAnchor;

import java.io.*;
import java.util.List;
import java.util.Set;

/**
 * Created by likaiqing on 16/11/07.
 */
public class IOTools {
    public static BufferedReader getBF(String file) {
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(new File(file)))));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return bf;
    }

    public static BufferedWriter getBW(String file) {
        BufferedWriter wb = null;
        try {
            wb = new BufferedWriter(new FileWriter(file));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wb;
    }

    public static void main(String[] args) {
        BufferedReader bf = getBF("/Users/likaiqing/Downloads/ruc-avatar.txt");
        BufferedWriter wb = getBW("/Users/likaiqing/Downloads/ruc-avatar.csv");
        String line = null;
        int index = 1;
        try {
            while ((line = bf.readLine()) != null) {
                String[] split = line.split(" ");
                wb.write(split[0] + "\u0001" + split[1]);
                wb.newLine();
                if (index++ % 10000 == 0) {
                    wb.flush();
                }
            }
            wb.flush();
            wb.close();
            bf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeList(List<String> results, BufferedWriter wb1) {
        try {
            for (String line : results) {
                wb1.write(line);
                wb1.newLine();
            }
            wb1.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeList(List<String> results, String dir) {
        try {
            BufferedWriter bw = getBW(dir);
            for (String line : results) {
                bw.write(line);
                bw.newLine();
            }
            bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeBw(BufferedWriter bw) {
        if (null !=  bw){
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void writeList(Set<DetailAnchor> detailAnchorSet, BufferedWriter bw) {
        try {
            for (DetailAnchor detailAnchor : detailAnchorSet) {
                bw.write(detailAnchor.toString());
                bw.newLine();
            }
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
