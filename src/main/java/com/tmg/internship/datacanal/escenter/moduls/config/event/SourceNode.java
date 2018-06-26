package com.tmg.internship.datacanal.escenter.moduls.config.event;

import com.tmg.internship.datacanal.escenter.common.EnumUtil;
import com.tmg.internship.datacanal.escenter.exception.ESConfigParseException;
import com.tmg.internship.datacanal.escenter.trigger.function.FunctionStrategy;
import com.tmg.internship.datacanal.escenter.trigger.function.FunctionType;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 合并数据的节点
 *
 * @author xiangjing
 * @date 2018/6/4
 * @company 天极云智
 */
public class SourceNode {

    private String indexName;//索引名称

    private String exportFields;//属性

    private List<Condition> conditionList;//查询条件

    private Boolean allowNull;//是否允许为空

    /**
     * key:columnName
     * value:FieldName
     */
    private LinkedHashMap<String, String> cols;

    /**
     * 原生的exportFields 的列名
     */
    private Map<String,List<String>> columns = null;


    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getExportFields() {
        return exportFields;
    }

    public void setExportFields(String exportFields) {

        this.exportFields = exportFields;
        handleExportFields();//处理数据
    }

    public List<Condition> getConditionList() {
        return conditionList;
    }

    public void setConditionList(List<Condition> conditionList) {
        this.conditionList = conditionList;
    }

    public Boolean getAllowNull() {
        return allowNull;
    }

    public void setAllowNull(Boolean allowNull) {
        this.allowNull = allowNull;
    }

    public LinkedHashMap<String, String> getCols() {
        return cols;
    }

    public void setCols(LinkedHashMap<String, String> cols) {
        this.cols = cols;
    }

    /**
     * 对列进行拆分
     * @param column
     * @return
     */
    public static String[] splitClumn(String column){

        String[] temp =  column.split("\\s+AS\\s+");
        if(temp.length == 1){
            temp = column.split("\\s+as\\s+");
        }
        return temp;
    }

     /**
     * 处理需要合并的列属性及其相应的属性
     */
    private void handleExportFields() {
        String exportFields = this.exportFields;
        cols = new LinkedHashMap<>();
        if(EnumUtil.isFunctionType(exportFields)){//不包含函数关键字
            FunctionType type = null;
            String regex = null;
            columns = new HashMap<>();
            for(int i=0;i<FunctionType.values().length;i++){
                type = FunctionType.values()[i];
                regex =FunctionStrategy.getFunctionRegex(type);
                exportFields=replaceRegex(exportFields,regex,type);
            }
            if(columns.size() == 0){//如果包含关键字有没有客源匹配的函数格式则说明函数格式错误
                throw new ESConfigParseException("["+this.exportFields+" ] exportFields,中函数格式错误!");
            }
        }
        String[] strs = exportFields.split(",");
        Set<String> replaces =new HashSet<>();
        if(null !=columns && columns.size()!=0){//有函数
            for(String key:columns.keySet()){
                int num=0;
                for(String value:columns.get(key)){
                    num++;
                    for (String col : strs) {
                        if(col.contains(key)){
                            if(col.matches(key+"_\\d+[\\sa-zA-Z\\d_]*")){//说明被替换
                                if(col.matches(key+"_"+num+"[\\sa-zA-Z\\d_]*")){
                                    replaces.add(col.replace(key + "_" + num, value));
                                }
                                if(!col.contains("AS") && !col.contains("as")){//验证是否给函数取了别名
                                    throw new ESConfigParseException("["+this.exportFields+" ] exportFields,中"+key+"函数没有取别名："+value);
                                }
                            }else{//说明包含函数关键字但又与函数格式不匹配
                                throw new ESConfigParseException("["+this.exportFields+" ] exportFields,中"+key+"函数格式错误："+col);
                            }
                        }else{//不包含函数关键字
                            replaces.add(col);
                        }
                    }
                }
            }
        }else{
            replaces.addAll(Arrays.asList(strs));
        }
        String[] temp = null;
        for (String col : replaces) {//将columnName 和fieldName 拆分
            temp =splitClumn(col);
            if (temp.length == 1) {
                cols.put(col.trim(), col.trim());
            } else {
                cols.put(temp[0].trim(),temp[1].trim());
            }
        }

    }

    /**
     * 正则替换
     * @param exportFields
     * @param regex 正则表达式
     * @param type 函数类型
     * @return
     */
    private String replaceRegex(String exportFields,String regex,FunctionType type){
        String fields = exportFields;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(exportFields);
       List<String> array = new ArrayList<>();
        int num=0;
        while(matcher.find()){
            array.add(matcher.group());
            num++;
        }
        if(num >0){
            columns.put(type.getCode(),array);
            for(int i=1;i<=num;i++){
                fields=pattern.matcher(fields).replaceFirst(type.getCode()+"_"+i);
            }
        }
        return fields;
    }

    @Override
    public String toString() {
        return "{" +
                "indexName='" + indexName + '\'' +
                ", exportFields='" + exportFields + '\'' +
                ", conditionList=" + conditionList +
                '}';
    }
}
