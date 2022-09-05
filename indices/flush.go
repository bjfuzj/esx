package esx_indices

import (
	"encoding/json"
	"fmt"

	"github.com/bjfuzj/esx"
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

func Flush(client *esx.Client, indexname string) bool {
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

func FlushWithPool(indexname string) bool {
	uri := fmt.Sprintf("%s/_flush", indexname)

	client := esx.Pool.Get()
	if client != nil {
		defer esx.Pool.Put(client)
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
