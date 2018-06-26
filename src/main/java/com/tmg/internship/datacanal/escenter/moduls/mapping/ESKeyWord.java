package com.tmg.internship.datacanal.escenter.moduls.mapping;

/**
 * Es 关键字
 *
 * @author xiangjing
 * @date 2018/5/9
 * @company 天极云智
 */
public class ESKeyWord {
    /**
     * 映射节点关键字
     */
    public enum Mapping{
        mapping,
        mappings,
        properties,
        type,
        fields,
        keyword,
        ignore_above,
        format,
        term_vector,
        fielddata;
    }

    /**
     * 自定义分析器部分关键字
     */
    public enum Setting{
        settings,//根节点
        analysis,//分析
        analyzer,//分析器
        tokenizer,//分词器
        number_of_shards,//主分片
        number_of_replicas;//副分片
    }

    /**
     * 自定义别名的根节点
     */
    public enum aliases{
        aliases;//别名根节点
    }

    /**
     * term_vector 的参数
     */
    public enum TermVector{
        no,
        yes,
        with_positions,
        with_offsets,
        with_positions_offsets

    }

}
