package esx

import (
	"encoding/json"
	"errors"
	"fmt"
)

type HealthResult struct {
	Status          string  `json:"status"`
	NumOfNodes      int     `json:"number_of_nodes"`
	NumOfDataNodes  int     `json:"number_of_data_nodes"`
	ActiveShards    int     `json:"active_shards"`
	ActivePriShards int     `json:"active_primary_shards"`
	RelocatShards   int     `json:"relocating_shards"`
	InitingShards   int     `json:"initializing_shards"`
	UnAssignShards  int     `json:"unassigned_shards"`
	DelayShards     int     `json:"delayed_unassigned_shards"`
	PendingTasks    int     `json:"number_of_pending_tasks"`
	InFlightFetch   int     `json:"number_of_in_flight_fetch"`
	TaskMaxWaitMs   int     `json:"task_max_waiting_in_queue_millis"`
	ActiveSdPct     float64 `json:"active_shards_percent_as_number"`
}

func Health(client *Client) (HealthResult, error) {
	uri := "_cluster/health"

	code, rdata := client.GetResponse("GET", uri, "", map[string]string{})
	if code >= 400 {
		return HealthResult{}, errors.New("查询ES集群状态失败")
	}

	var resp HealthResult
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return HealthResult{}, fmt.Errorf("查询ES集群状态的响应结果解析失败: %s", err.Error())
	}

	return resp, nil
}

func HealthWithPool() (HealthResult, error) {
	client := Pool.Get()
	if client != nil {
		defer Pool.Put(client)
	}

	return Health(client)
}
