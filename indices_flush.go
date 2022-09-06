package esx

import (
	"encoding/json"
	"fmt"
)

// {
// 	"_shards" : {
// 		"total" : 2,
// 		"successful" : 2,
// 		"failed" : 0
// 	}
// }

type flushRespTemp struct {
	Shards struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Status int `json:"status"`
}

// Flush 刷新index
// true => 成功, false => 失败
func Flush(client *Client, indexname string) bool {
	uri := fmt.Sprintf("%s/_flush", indexname)

	code, rdata := client.GetResponse("POST", uri, "", map[string]string{})
	if code >= 400 {
		return false
	}

	var resp flushRespTemp
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return false
	}

	if resp.Status >= 400 {
		return false
	}

	if resp.Shards.Failed > 0 {
		return false
	}

	return true
}

// FlushWithPool 刷新index
// true => 成功, false => 失败
func FlushWithPool(indexname string) bool {
	uri := fmt.Sprintf("%s/_flush", indexname)

	client := Pool.Get()
	if client != nil {
		defer Pool.Put(client)
	}

	code, rdata := client.GetResponse("POST", uri, "", map[string]string{})
	if code >= 400 {
		return false
	}

	var resp flushRespTemp
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return false
	}

	if resp.Status >= 400 {
		return false
	}

	if resp.Shards.Failed > 0 {
		return false
	}

	return true
}
