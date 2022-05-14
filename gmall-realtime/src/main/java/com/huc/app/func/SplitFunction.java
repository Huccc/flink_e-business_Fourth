package com.huc.app.func;

import com.huc.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String keyword) {
        List<String> words = null;
        try {
            words = KeyWordUtil.splitKeyWord(keyword);

            for (String word : words) {
                collect(Row.of(word));
            }
        } catch (Exception e) {
            collect(Row.of(keyword));
        }
    }
}
