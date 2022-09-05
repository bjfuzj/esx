package esx_indices

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bjfuzj/esx"
)

func AliasAction(client *esx.Client, aliasname string, actions []map[string]map[string]string) error {
	reqdata := map[string][]map[string]map[string]string{
		"actions": actions,
	}
	reqbody, _ := json.Marshal(reqdata)

	code, rdata := client.GetResponse("POST", "_aliases", string(reqbody), map[string]string{})
	if code >= 400 {
		return fmt.Errorf("执行alias[%s]变更失败", aliasname)
	}

	var resp esx.Ack
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return fmt.Errorf("变更alias[%s]的响应结果解析失败: %s", aliasname, err.Error())
	}

	if !resp.Acknowledged {
		return fmt.Errorf("执行alias[%s]变更失败, acknowledged = false", aliasname)
	}

	return nil
}

func AliasQuery(client *esx.Client, aliasname string) ([]string, error) {
	uri := fmt.Sprintf("_alias/%s", aliasname)
	code, rdata := client.GetResponse("GET", uri, "", map[string]string{})
	if code >= esx.HTTP_SELF_CODE {
		return nil, errors.New("发起alias请求失败")
	}

	var resp map[string]any
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return nil, fmt.Errorf("查询alias[%s]结果解析失败: %s", aliasname, err.Error())
	}

	if status, isE := resp["status"]; isE {
		si, ok := status.(int)
		if ok && si >= 400 {
			return nil, fmt.Errorf("查询alias[%s]失败: %v", aliasname, resp["error"])
		}
	}

	ret := make([]string, 0)
	for k := range resp {
		ret = append(ret, k)
	}

	return ret, nil
}

func AliasActionWithPool(aliasname string, actions []map[string]map[string]string) error {
	client := esx.Pool.Get()
	if client != nil {
		defer esx.Pool.Put(client)
	}

	reqdata := map[string][]map[string]map[string]string{
		"actions": actions,
	}
	reqbody, _ := json.Marshal(reqdata)

	code, rdata := client.GetResponse("POST", "_aliases", string(reqbody), map[string]string{})
	if code >= 400 {
		return fmt.Errorf("执行alias[%s]变更失败", aliasname)
	}

	var resp esx.Ack
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return fmt.Errorf("变更alias[%s]的响应结果解析失败: %s", aliasname, err.Error())
	}

	if !resp.Acknowledged {
		return fmt.Errorf("执行alias[%s]变更失败, acknowledged = false", aliasname)
	}

	return nil
}

func AliasQueryWithPool(aliasname string) ([]string, error) {
	client := esx.Pool.Get()
	if client != nil {
		defer esx.Pool.Put(client)
	}

	uri := fmt.Sprintf("_alias/%s", aliasname)
	code, rdata := client.GetResponse("GET", uri, "", map[string]string{})
	if code >= esx.HTTP_SELF_CODE {
		return nil, errors.New("发起alias请求失败")
	}

	var resp map[string]any
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return nil, fmt.Errorf("查询alias[%s]结果解析失败: %s", aliasname, err.Error())
	}

	if status, isE := resp["status"]; isE {
		si, ok := status.(int)
		if ok && si >= 400 {
			return nil, fmt.Errorf("查询alias[%s]失败: %v", aliasname, resp["error"])
		}
	}

	ret := make([]string, 0)
	for k := range resp {
		ret = append(ret, k)
	}

	return ret, nil
}
