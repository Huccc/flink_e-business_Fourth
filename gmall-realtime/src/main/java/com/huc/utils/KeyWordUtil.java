package com.huc.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc: IK分词器工具类
 */
public class KeyWordUtil {
    // 使用IK分词器对字符串进行分词
    public static List<String> splitKeyWord(String keyword) throws Exception {
        // 创建集合用于存放最终结果数据
        ArrayList<String> list = new ArrayList<>();

        // 创建Ik分词器对象
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        Lexeme lexeme = ikSegmenter.next();

        while (lexeme != null) {
            String lexemeText = lexeme.getLexemeText();
            list.add(lexemeText);

            lexeme = ikSegmenter.next();
        }

        // 返回结果数据
        return list;
    }

    public static void main(String[] args) throws Exception {
        List<String> splitKeyWord = splitKeyWord("孙悟空大闹天宫");

        for (String word : splitKeyWord) {
            System.out.println(word);
        }
    }
}
