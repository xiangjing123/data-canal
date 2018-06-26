package com.tmg.internship.datacanal.escenter.moduls.config;

import com.tmg.internship.datacanal.escenter.common.BaseClass;
import com.tmg.utils.StringUtils;

import java.util.List;

/**
 * 对应ES mapping 节点 表示数据域的配置
 *
 * @author xiangjing
 * @date 2018/5/11
 * @company 天极云智
 */
public class MappingNode  extends BaseClass {

    private String schemaName; //schemaName 用于定义数据域，如：数据库名，日志类型名(runtime_log,business_log)等

    private List<String> ikKSmart;//设置数据域的全局的中文分词（按词义）的字段名称

    private List<String> ikMaxWord;//设置数据域的全局的使用ik_max_word中文分词 的字段名称（不按词义，按最大单词来分）

    private List<String> english;//设置数据域的全局的采用english 的分词的字段名称

    private List<String> pathHierarchy;//设置采用路径分词（path_hierarchy）的字段名称

    private List<CollectionNode> collectionNodes;//collection 用于定义数据域下的同类数据集，如：数据库表，日志文件等

    /**
     *获取数据域的名称
     * @return
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * 设置数据域的名称
     * @param schemaName
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * 获取需要采用 ik_smart 分词的字段
     * @return
     */
    public List<String> getIkKSmart() {
        return ikKSmart;
    }

    /**
     * 设置需要采用 ik_smart 分词的字段
     * @param  ikKSmart 字段域名称集合
     *
     */
    public void setIkKSmart(List<String> ikKSmart) {
        this.ikKSmart = ikKSmart;
    }


    /**
     * 获取需要采用 ik_max_word 分词的字段
     * @return 需要设置为 ik_max_word 的 字段的集合
     */
    public List<String> getIkMaxWord() {
        return ikMaxWord;
    }

    /**
     * 设置需要采用ik_max_word 分词的字段
     * @param  ikMaxWord 字段名称集合
     *
     */
    public void setIkMaxWord(List<String> ikMaxWord) {
        this.ikMaxWord = ikMaxWord;
    }

    /**
     * 获取需要采用 english 分词的字段
     * @return 需要设置为 english 的 字段的集合
     */
    public List<String> getEnglish() {
        return english;
    }

    /**
     * 设置需要采用 english 分词的字段
     * @param  english 字段名称集合
     *
     */
    public void setEnglish(List<String> english) {
        this.english = english;
    }

    /**
     * 获取需要采用 path_hierarchy 分词的字段
     * @return 需要设置为 path_hierarchy 的 字段的集合
     */

    public List<String> getPathHierarchy() {
        return pathHierarchy;
    }

    /**
     * 设置需要采用 path_hierarchy 分词的字段
     * @param  pathHierarchy 字段名称集合
     *
     */
    public void setPathHierarchy(List<String> pathHierarchy) {
        this.pathHierarchy = pathHierarchy;
    }

    /**
     * 获取数据域下面的数据集的配置
     * @return
     */
    public List<CollectionNode> getCollectionNodes() {
        return collectionNodes;
    }

    /**
     * 设置数据域下面的数据集的配置
     * @param collectionNodes
     */
    public void setCollectionNodes(List<CollectionNode> collectionNodes) {
        this.collectionNodes = collectionNodes;
    }


    /**
     * 根据数据集的名称获取ES配置里面数据域中数据集的配置
     * @param name 数据集名称
     * @return
     */
    public CollectionNode getCollectionNode(String name){
        if(StringUtils.isEmpty(name)){
            return null;
        }

        if(null == this.collectionNodes || this. collectionNodes.size() == 0){
            return null;
        }
        for(CollectionNode collectionNode:collectionNodes){
            if(collectionNode.getName().equals(name)){
                return collectionNode;
            }else{
                String collectiongName = collectionNode.getName();
                if(collectiongName.contains("*")){
                    collectiongName =collectiongName.replace("*","[_\\dA-Za-z]*")+"$";
                    if(name.matches(collectiongName)){
                        return collectionNode;
                    }

                }
            }
        }
        return null;
    }

    /**
     * 通过field 在mapping 全局分词器设置里面寻找对应的分词器
     * @param fieldName
     * @return
     */
    public Tokenizer getWholeTokenizer(String fieldName){

        if( null != this.ikKSmart && this.ikKSmart !=null){
            for(String field:this.ikKSmart){
                if(field.equals(fieldName)){
                    return Tokenizer.IK_SMART;
                }
            }
        }

        if( null != this.ikMaxWord && this.ikMaxWord !=null){
            for(String field:this.ikMaxWord){
                if(field.equals(fieldName)){
                    return Tokenizer.IK_MAX_WORD;
                }
            }
        }

        if( null != this.pathHierarchy && this.pathHierarchy !=null){
            for(String field:this.pathHierarchy){
                if(field.equals(fieldName)){
                    return Tokenizer.PATH;
                }
            }
        }
        if( null != this.english && this.english !=null){
            for(String field:this.english){
                if(field.equals(fieldName)){
                    return Tokenizer.ENGLISH;
                }
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "{" +
                "schemaName='" + schemaName + '\'' +
                ", ikKSmart=" + ikKSmart +
                ", ikMaxWord=" + ikMaxWord +
                ", english=" + english +
                ", pathHierarchy=" + pathHierarchy +
                ", collectionNodes=" + collectionNodes +
                '}';
    }
}
