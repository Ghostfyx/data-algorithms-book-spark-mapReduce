package org.algorithms.orderinversion.mapreduce;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-14
 */
public class Test {
    public static void main(String[] args) {
        String value = "java is a language";
        int neighborWindow = 2;
        String[] tokens = value.toString().split(" ");
        if ((tokens == null) || (tokens.length < 2)) {
            return;
        }
        for (int i = 0; i < tokens.length; i++) {
            // replaceAll regex的用法见第五章对应笔记
            tokens[i] = tokens[i].replaceAll("\\W+", "");
            if (tokens[i].equals("")) {
                continue;
            }
            String word = tokens[i];
            // 获取当前元素的左右窗口
            int start = (i - neighborWindow < 0) ? 0 : i - neighborWindow;
            int end = (i + neighborWindow >= tokens.length - 1) ? tokens.length -1 : i + neighborWindow;
            for (int j = start; j <= end; j++) {
//                当前元素左右窗口包含的元素 (W_i, W_start),(W_i, W_start+1),....,(W_i, W_end)
                if (j == i) {
                    continue;
                }
                System.out.println("left:"+word +", right:"+tokens[j].replaceAll("\\W", ""));
            }
        }
    }
}
