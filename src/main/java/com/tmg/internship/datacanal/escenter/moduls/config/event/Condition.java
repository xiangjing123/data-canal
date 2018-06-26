package com.tmg.internship.datacanal.escenter.moduls.config.event;

/**
 * 条件
 *
 * @author xiangjing
 * @date 2018/6/4
 * @company 天极云智
 */
public class Condition {

    private String columnName;//源数据 列的值

    private String fieldName;//<!-- 取级联索引及字段名为student_num的值  如：t_internship_journal.student_num-->

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String toString() {
        return "{" +
                "columnName='" + columnName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }
}
