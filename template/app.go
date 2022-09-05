package esx_template

import (
	"fmt"

	"github.com/bjfuzj/esx"
)

func Exist(client *esx.Client, name string) bool {
	uri := fmt.Sprintf("_template/%s", name)

	code, _ := client.GetResponse("GET", uri, "", map[string]string{})
	return code < 400
}

func Create(client *esx.Client, name string, qdata []byte) error {
	uri := fmt.Sprintf("_template/%s", name)

	code, rdata := client.GetResponse("PUT", uri, string(qdata), map[string]string{})
	if code >= 400 {
		return fmt.Errorf("创建模板[%s]失败, 返回的响应是: %s", name, string(rdata))
	}

	return nil
}

func ExistWithPool(name string) bool {
	uri := fmt.Sprintf("_template/%s", name)

	client := esx.Pool.Get()
	if client != nil {
		defer esx.Pool.Put(client)
	}

	code, _ := client.GetResponse("GET", uri, "", map[string]string{})
	return code < 400
}

func CreateWithPool(name string, qdata []byte) error {
	uri := fmt.Sprintf("_template/%s", name)

	client := esx.Pool.Get()
	if client != nil {
		defer esx.Pool.Put(client)
	}

	code, rdata := client.GetResponse("PUT", uri, string(qdata), map[string]string{})
	if code >= 400 {
		return fmt.Errorf("创建模板[%s]失败, 返回的响应是: %s", name, string(rdata))
	}

	return nil
}
