package com.trivadis;

import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.CsvNumber;

public class ControlDataDO {
    @CsvBindByPosition(position = 0)
    private String architimetext;

    @CsvBindByPosition(position = 1)
    private String b1;

    @CsvBindByPosition(position = 2)
    private String b2;

    @CsvBindByPosition(position = 3)
    private String b3;

    @CsvBindByPosition(position = 4)
    private String elem;

    @CsvBindByPosition(position = 5)
    private String info;

    @CsvBindByPosition(position = 6)
    private String resolution;

    @CsvBindByPosition(position = 7)
    @CsvNumber("###,######")
    private Double value;

    @CsvBindByPosition(position = 8)
    private String flag;

    public String getArchitimetext() {
        return architimetext;
    }

    public String getB1() {
        return b1;
    }

    public String getB2() {
        return b2;
    }

    public String getB3() {
        return b3;
    }

    public String getElem() {
        return elem;
    }

    public String getInfo() {
        return info;
    }

    public String getResolution() {
        return resolution;
    }

    public Double getValue() {
        return value;
    }

    public String getFlag() {
        return flag;
    }

    @Override
    public String toString() {
        return "ControlDataDO{" +
                "architimetext='" + architimetext + '\'' +
                ", b1='" + b1 + '\'' +
                ", b2='" + b2 + '\'' +
                ", b3='" + b3 + '\'' +
                ", elem='" + elem + '\'' +
                ", info='" + info + '\'' +
                ", resolution='" + resolution + '\'' +
                ", value=" + value +
                ", flag='" + flag + '\'' +
                '}';
    }
}
