package esx

import (
	"encoding/json"
	"errors"
	"fmt"
)

type IndexInfo struct {
	Status string `json:"status"`
	Index  string `json:"index"`
}

// Cat 查看index信息列表
func Cat(client *Client, pattern string) ([]IndexInfo, error) {
	uri := "_cat/indices?s=index&format=json"
	if pattern != "" {
		uri = fmt.Sprintf("%s&index=%s", uri, pattern)
	}

	code, rdata := client.GetResponse("GET", uri, "", map[string]string{})
	if code >= 400 {
		return nil, errors.New("查询ES索引列表失败")
	}

	var resp []IndexInfo
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return nil, fmt.Errorf("查询ES索引列表的响应结果解析失败: %s", err.Error())
	}

	return resp, nil
}

func CatWithPool(pattern string) ([]IndexInfo, error) {
	client := Pool.Get()
	if client != nil {
		defer Pool.Put(client)
	}

	return Cat(client, pattern)
}
