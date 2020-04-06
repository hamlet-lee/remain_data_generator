package com.xinitek;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.FileNotFoundException;
import java.io.PrintStream;

public class DateGenerator {
    public static void main(String[] args) throws FileNotFoundException {
        PrintStream ps = new PrintStream("dim_date.tsv");
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        DateTime start = formatter.parseDateTime("1980-01-01");
        DateTime end = formatter.parseDateTime("3000-01-01");
        for(;start.isBefore(end); start = start.plusDays(1) ) {
            ps.println(formatter.print(start));
        }
        ps.close();
    }
}
