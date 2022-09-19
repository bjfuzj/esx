package esx

import (
	"encoding/json"
)

// Del 删除指定index
// 返回删除失败的index列表
func Del(client *Client, indexList []string) []string {
	ret := make([]string, 0)

	for _, indexname := range indexList {
		code, rdata := client.GetResponse("DELETE", indexname, "", map[string]string{})
		if code >= 400 {
			ret = append(ret, indexname)
			continue
		}

		var resp Ack
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

// DelWithPool 删除指定index
// 返回删除失败的index列表
func DelWithPool(indexList []string) []string {
	client := Pool.Get()
	if client != nil {
		defer Pool.Put(client)
	}

	return Del(client, indexList)
}
