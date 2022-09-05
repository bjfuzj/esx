package esx_search

import (
	"encoding/json"
	"errors"
	"fmt"
)

type HitsResponse struct {
	Index  string         `json:"_index"`
	Score  float64        `json:"_score"`
	Source map[string]any `json:"_source"`
}

type SearchResponse struct {
	Timeout bool `json:"timed_out"`
	Shards  struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
	} `json:"_shards"`
	Status int `json:"status"`
	Hits   struct {
		Total int64          `json:"total"`
		Hits  []HitsResponse `json:"hits"`
	} `json:"hits"`
	Aggs map[string]json.RawMessage `json:"aggregations"`
}

type SearchResult struct {
	Total int64            `json:"total"`
	Hits  []map[string]any `json:"hits"`
	Aggs  []map[string]any `json:"aggs"`
}

func (c *SearchResult) Tostring() string {
	// res, _ := json.Marshal(c)
	res, _ := json.MarshalIndent(c, "", "  ")
	return string(res)
}

type ReqParam struct {
	Terms []string
}

type bucketsTemp struct {
	Buckets []map[string]json.RawMessage `json:"buckets"`
}

func ParseSearchResult(in []byte, req *EasyRequest) (SearchResult, error) {
	var err error
	ret := SearchResult{}

	// 反序列化
	var resp SearchResponse
	err = json.Unmarshal(in, &resp)
	if err != nil {
		return ret, err
	}

	// 组装解析参数
	terms := make([]string, 0)
	funcs := make([]string, 0)
	toggle := false

	for _, x := range req.Terms {
		terms = append(terms, fmt.Sprintf("terms_%s", x))
	}
	if len(req.Interval) > 0 {
		terms = append(terms, fmt.Sprintf("terms_%s", req.TimeField))
	}

	for k, v := range req.Funcs {
		for _, vv := range v {
			funcs = append(funcs, fmt.Sprintf("%s_%s", k, vv))
		}
	}
	if req.ScriptedMetric != nil {
		funcs = append(funcs, "scripted_metric")
		toggle = req.ScriptedMetric.Toggle
	}

	aggs, err := parseAggs(resp.Aggs, 0, toggle, terms, funcs)
	if err != nil {
		return ret, fmt.Errorf("聚合数据递归解析失败: %s", err.Error())
	}

	ret.Total = resp.Hits.Total
	ret.Hits = make([]map[string]any, 0)
	for _, x := range resp.Hits.Hits {
		ret.Hits = append(ret.Hits, x.Source)
	}
	ret.Aggs = aggs

	return ret, nil
}

func parseLeaf(aggdatas map[string]json.RawMessage, toggle bool, funcs []string) (map[string]any, error) {
	ret := make(map[string]any)
	for _, k := range funcs {
		rawvalue := aggdatas[k]
		var datas map[string]any
		err := json.Unmarshal(rawvalue, &datas)
		if err != nil {
			return nil, err
		}

		if toggle && k == "scripted_metric" {
			v, ok := datas["value"].(map[string]any)
			if !ok {
				return nil, errors.New("scripted_metric展开断言失败")
			}
			for kk, vv := range v {
				ret[kk] = vv
			}
		} else {
			ret[k] = datas["value"]
		}
	}

	return ret, nil
}

func parseAggs(aggdatas map[string]json.RawMessage, idx int, toggle bool, terms, funcs []string) ([]map[string]any, error) {
	// 递归
	ret := make([]map[string]any, 0)
	var err error

	if len(terms) == 0 {
		res, err := parseLeaf(aggdatas, toggle, funcs)
		if err != nil {
			return nil, err
		}
		ret = append(ret, res)
		return ret, nil
	}

	key := terms[idx]
	rawvalue := aggdatas[key]
	var datas bucketsTemp
	err = json.Unmarshal(rawvalue, &datas)
	if err != nil {
		return nil, err
	}

	idx++
	if idx == len(terms) {
		// 到末尾了
		for _, x := range datas.Buckets {
			res, err := parseLeaf(x, toggle, funcs)
			if err != nil {
				return nil, err
			}
			var keyValue, ctValue any
			if err = json.Unmarshal(x["key"], &keyValue); err == nil {
				res[key] = keyValue
			} else {
				continue
			}
			if err = json.Unmarshal(x["doc_count"], &ctValue); err == nil {
				res["ct"] = ctValue
			} else {
				continue
			}

			ret = append(ret, res)
		}

		return ret, nil
	}

	for _, x := range datas.Buckets {
		res, err := parseAggs(x, idx, toggle, terms, funcs)
		if err != nil {
			return nil, err
		}
		for _, y := range res {
			var keyValue any
			if err = json.Unmarshal(x["key"], &keyValue); err == nil {
				y[key] = keyValue
				ret = append(ret, y)
			}
		}
	}

	return ret, nil
}
