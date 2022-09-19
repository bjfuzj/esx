package esx

import (
	"fmt"
)

// Exist 判断index是否存在
// true => 存在, false => 不存在
func Exist(client *Client, indexname string) bool {
	uri := fmt.Sprintf("_cat/indices?index=%s&format=json", indexname)

	code, _ := client.GetResponse("GET", uri, "", map[string]string{})
	return code < 400
}

// ExistWithPool 判断index是否存在
// true => 存在, false => 不存在
func ExistWithPool(indexname string) bool {
	client := Pool.Get()
	if client != nil {
		defer Pool.Put(client)
	}

	return Exist(client, indexname)
}
