package esx

import (
	"encoding/json"
)

// Create 创建指定index
func Create(client *Client, indexname string) bool {
	if Exist(client, indexname) {
		return true
	}

	code, rdata := client.GetResponse("PUT", indexname, "", map[string]string{})
	if code >= 400 {
		return false
	}

	var resp Ack
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return false
	}

	return resp.Acknowledged
}

// CreateWithPool 创建指定index
func CreateWithPool(indexname string) bool {
	if ExistWithPool(indexname) {
		return true
	}

	client := Pool.Get()
	if client != nil {
		defer Pool.Put(client)
	}

	code, rdata := client.GetResponse("PUT", indexname, "", map[string]string{})
	if code >= 400 {
		return false
	}

	var resp Ack
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return false
	}

	return resp.Acknowledged
}
