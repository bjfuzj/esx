package esx_indices

import (
	"encoding/json"

	"github.com/bjfuzj/esx"
)

func Create(client *esx.Client, indexname string) bool {
	if Exist(client, indexname) {
		return true
	}

	code, rdata := client.GetResponse("PUT", indexname, "", map[string]string{})
	if code >= 400 {
		return false
	}

	var resp esx.Ack
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return false
	}

	return resp.Acknowledged
}

func CreateWithPool(indexname string) bool {
	if ExistWithPool(indexname) {
		return true
	}

	client := esx.Pool.Get()
	if client != nil {
		defer esx.Pool.Put(client)
	}

	code, rdata := client.GetResponse("PUT", indexname, "", map[string]string{})
	if code >= 400 {
		return false
	}

	var resp esx.Ack
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return false
	}

	return resp.Acknowledged
}
