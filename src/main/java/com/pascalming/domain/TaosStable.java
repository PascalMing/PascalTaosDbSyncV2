package com.pascalming.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * TaosStable
 *
 * @author pascal
 * @date 2021-05-27
 * @description: 超级表数据
 */

public class TaosStable {
    public String name;
    public String created_time;
    public int columns;
    public int tags;
    public int tables;
    public long sourceCount;
    public long destinationCount;
    public long insertCount;

    public List<String> Field=new ArrayList<>();
    public List<String> Type=new ArrayList<>();
    public List<Integer> Length=new ArrayList<>();
    public List<String> Note = new ArrayList<>();

    @Override
    public String toString() {
        return "TaosStable{" +
                "name='" + name + '\'' +
                ", created_time='" + created_time + '\'' +
                ", columns=" + columns +
                ", tags=" + tags +
                ", tables=" + tables +
                '}';
    }
}
