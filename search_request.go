package esx

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// rest_total_hits_as_int=true
// search_type=query_then_fetch
// ignore_unavailable=true

// type MultiSearchResponse struct {
// 	Responses []SearchResponse `json:"responses"`
// }

// ScriptedMetric 提供给scripted_metric专用结构体
type ScriptedMetric struct {
	Toggle        bool           `json:"toggle,omitempty"` // 定义是否展开script结果集, 默认不展开, 通常只有结果是单层map如 map[string]int 时才有展开需求
	Params        map[string]any `json:"params,omitempty"`
	InitScript    string         `json:"init_script"`
	MapScript     string         `json:"map_script"`
	CombineScript string         `json:"combine_script"`
	ReduceScript  string         `json:"reduce_script"`
}

// EasyRequest 简易的ES请求结构体, 会自动封装实际的DSL
type EasyRequest struct {
	Size           int64               `json:"size"`
	PreIndex       string              `json:"preIndex"`
	TimePattern    string              `json:"timePattern"`
	TimeField      string              `json:"timeField" default:"@timestamp"`
	TimeShift      int64               `json:"timeShift"` // 本地的偏移时区, 比如UTC就是-8
	TimeFrom       int64               `json:"timeFrom"`
	TimeTill       int64               `json:"timeTill"`
	Interval       string              `json:"interval"`
	QueryString    string              `json:"querystring"`
	Funcs          map[string][]string `json:"funcs"`
	Terms          []string            `json:"terms"`
	Filters        map[string]any      `json:"filters"`
	Shoulds        []map[string]any    `json:"shoulds"`
	MustNot        []map[string]any    `json:"must_not"`
	ScriptedMetric *ScriptedMetric     `json:"scripted_metric,omitempty"`
}

// SearchRequest 实际的DSL请求结构体
type SearchRequest struct {
	Indices []string `json:"-"`
	Size    int64    `json:"size"`
	Query   struct {
		Bool struct {
			Filter             []map[string]any `json:"filter,omitempty"`
			Must               []map[string]any `json:"must,omitempty"` // v0.1版本不提供must
			MustNot            []map[string]any `json:"must_not,omitempty"`
			Should             []map[string]any `json:"should,omitempty"`
			MinimumShouldMatch int              `json:"minimum_should_match,omitempty"`
		} `json:"bool,omitempty"`
	} `json:"query,omitempty"`
	Aggs map[string]map[string]any `json:"aggs,omitempty"`
}

func (c *SearchRequest) Tostring() string {
	// res, _ := json.Marshal(c)
	res, _ := json.MarshalIndent(c, "", "  ")
	return string(res)
}

func NewSearchRequest(in *EasyRequest) (SearchRequest, error) {
	ret := SearchRequest{
		Size:    in.Size,
		Indices: make([]string, 0),
	}

	// if in.TimeFrom >= in.TimeTill {
	// 	return ret, errors.New("请指定正确的时间范围")
	// }

	// 先计算待查询的indices
	// 计算时区偏移
	// 注意timeFrom 和 timeTill 需要是毫秒值
	if in.TimeFrom == 0 || in.TimeTill == 0 {
		// 只要有一个没给出, 就无法计算index范围
		// 除非额外请求_cat/indices获取所有列表
		// 所以直接使用wildcard模式
		ret.Indices = append(ret.Indices, fmt.Sprintf("%s-*", in.PreIndex))
	} else {
		timeFrom := in.TimeFrom + 3600*1000*in.TimeShift
		timeTill := in.TimeTill + 3600*1000*in.TimeShift
		indices := make(map[string]uint8, 0)
		for timeFrom < timeTill {
			indexsuffix := time.UnixMilli(timeFrom).Format(GetTimeTemps(in.TimePattern))
			key := fmt.Sprintf("%s-%s", in.PreIndex, indexsuffix)
			indices[key] = 1
			timeFrom += 86400 * 1000
		}
		indexsuffix := time.UnixMilli(timeTill).Format(GetTimeTemps(in.TimePattern))
		key := fmt.Sprintf("%s-%s", in.PreIndex, indexsuffix)
		indices[key] = 1

		if len(indices) == 0 {
			return ret, errors.New("待查询的index列表为空")
		}
		for k := range indices {
			ret.Indices = append(ret.Indices, k)
		}
	}

	// filter过滤条件组装
	filters := make([]map[string]any, 0)

	// 判断range条件
	timeRange := make(map[string]int64)
	if in.TimeFrom > 0 {
		timeRange["gte"] = in.TimeFrom
	}
	if in.TimeTill > 0 {
		timeRange["lt"] = in.TimeTill
	}
	if len(timeRange) > 0 {
		filters = append(filters, map[string]any{
			"range": map[string]map[string]int64{
				in.TimeField: timeRange,
			},
		})
	}

	if len(in.QueryString) > 0 {
		filters = append(filters, map[string]any{
			"query_string": map[string]any{
				"query":            in.QueryString,
				"analyze_wildcard": true,
			},
		})
	}

	for k, v := range in.Filters {
		filters = append(filters, map[string]any{
			"term": map[string]any{
				k: v,
			},
		})
	}

	if len(filters) > 0 {
		ret.Query.Bool.Filter = filters
	}

	// should过滤条件组装
	shoulds := make([]map[string]any, 0)
	for _, x := range in.Shoulds {
		shoulds = append(shoulds, map[string]any{
			"term": x,
		})
	}
	if len(shoulds) > 0 {
		ret.Query.Bool.Should = shoulds
		ret.Query.Bool.MinimumShouldMatch = 1
	}

	// must_not过滤条件组装
	mustnots := make([]map[string]any, 0)
	for _, x := range in.MustNot {
		mustnots = append(mustnots, map[string]any{
			"term": x,
		})
	}
	if len(mustnots) > 0 {
		ret.Query.Bool.MustNot = mustnots
	}

	// 聚合条件组装
	// sum / avg / max / min
	aggs := make(map[string]map[string]any)
	for k, v := range in.Funcs {
		for _, vv := range v {
			key := fmt.Sprintf("%s_%s", k, vv)
			aggs[key] = map[string]any{
				k: map[string]string{
					"field": vv,
				},
			}
		}
	}
	// scripted_metric
	if in.ScriptedMetric != nil {
		aggs["scripted_metric"] = map[string]any{
			"scripted_metric": ScriptedMetric{
				Params:        in.ScriptedMetric.Params,
				InitScript:    in.ScriptedMetric.InitScript,
				MapScript:     in.ScriptedMetric.MapScript,
				CombineScript: in.ScriptedMetric.CombineScript,
				ReduceScript:  in.ScriptedMetric.ReduceScript,
			},
		}
	}

	// date_histogram 看成是特殊的terms分桶字段
	terms := make([]string, 0)
	terms = append(terms, in.Terms...)
	if len(in.Interval) > 0 {
		terms = append(terms, in.TimeField)
	}

	idx := len(terms) - 1
	termsTimeField := fmt.Sprintf("terms_%s", in.TimeField)
	for idx >= 0 {
		x := terms[idx]
		idx--

		key := fmt.Sprintf("terms_%s", x)
		var tmp map[string]map[string]any

		if key == termsTimeField {
			tmp = map[string]map[string]any{
				key: {
					"date_histogram": map[string]any{
						"field":          in.TimeField,
						"fixed_interval": in.Interval,
						"time_zone":      "Asia/Shanghai",
						"min_doc_count":  1,
					},
				},
			}
		} else {
			tmp = map[string]map[string]any{
				key: {
					"terms": map[string]any{
						"field": x,
						"size":  10000,
					},
				},
			}
		}

		if len(aggs) > 0 {
			tmp[key]["aggs"] = aggs
		}
		aggs = tmp
	}
	if len(aggs) > 0 {
		ret.Aggs = aggs
	}

	return ret, nil
}
