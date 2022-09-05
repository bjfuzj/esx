package esx_indices

import (
	"fmt"

	"github.com/bjfuzj/esx"
)

// Exist 判断index是否存在
// true => 存在, false => 不存在
func Exist(client *esx.Client, indexname string) bool {
	uri := fmt.Sprintf("_cat/indices?index=%s&format=json", indexname)

	code, _ := client.GetResponse("GET", uri, "", map[string]string{})
	return code < 400
}

func ExistWithPool(indexname string) bool {
	uri := fmt.Sprintf("_cat/indices?index=%s&format=json", indexname)

	client := esx.Pool.Get()
	if client != nil {
		defer esx.Pool.Put(client)
	}

	code, _ := client.GetResponse("GET", uri, "", map[string]string{})
	return code < 400
}
