package esx_indices

import (
	"encoding/json"

	"github.com/bjfuzj/esx"
)

func Del(client *esx.Client, indexList []string) []string {
	ret := make([]string, 0)

	for _, indexname := range indexList {
		code, rdata := client.GetResponse("DELETE", indexname, "", map[string]string{})
		if code >= 400 {
			ret = append(ret, indexname)
			continue
		}

		var resp esx.Ack
		err := json.Unmarshal(rdata, &resp)
		if err != nil {
			ret = append(ret, indexname)
			continue
		}

		if !resp.Acknowledged {
			ret = append(ret, indexname)
		}
	}

	return ret
}

func DelWithPool(indexList []string) []string {
	client := esx.Pool.Get()
	if client != nil {
		defer esx.Pool.Put(client)
	}

	ret := make([]string, 0)

	for _, indexname := range indexList {
		code, rdata := client.GetResponse("DELETE", indexname, "", map[string]string{})
		if code >= 400 {
			ret = append(ret, indexname)
			continue
		}

		var resp esx.Ack
		err := json.Unmarshal(rdata, &resp)
		if err != nil {
			ret = append(ret, indexname)
			continue
		}

		if !resp.Acknowledged {
			ret = append(ret, indexname)
		}
	}

	return ret
}
