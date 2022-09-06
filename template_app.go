package esx

import (
	"fmt"
)

// TempExist 判断模板是否存在
// true => 存在, false => 不存在
func TempExist(client *Client, name string) bool {
	uri := fmt.Sprintf("_template/%s", name)

	code, _ := client.GetResponse("GET", uri, "", map[string]string{})
	return code < 400
}

// TempCreate 创建模板
func TempCreate(client *Client, name string, qdata []byte) error {
	uri := fmt.Sprintf("_template/%s", name)

	code, rdata := client.GetResponse("PUT", uri, string(qdata), map[string]string{})
	if code >= 400 {
		return fmt.Errorf("创建模板[%s]失败, 返回的响应是: %s", name, string(rdata))
	}

	return nil
}

// TempExistWithPool 判断模板是否存在
// true => 存在, false => 不存在
func TempExistWithPool(name string) bool {
	uri := fmt.Sprintf("_template/%s", name)

	client := Pool.Get()
	if client != nil {
		defer Pool.Put(client)
	}

	code, _ := client.GetResponse("GET", uri, "", map[string]string{})
	return code < 400
}

// TempCreateWithPool 创建模板
func TempCreateWithPool(name string, qdata []byte) error {
	uri := fmt.Sprintf("_template/%s", name)

	client := Pool.Get()
	if client != nil {
		defer Pool.Put(client)
	}

	code, rdata := client.GetResponse("PUT", uri, string(qdata), map[string]string{})
	if code >= 400 {
		return fmt.Errorf("创建模板[%s]失败, 返回的响应是: %s", name, string(rdata))
	}

	return nil
}
