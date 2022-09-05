package esx_bulk

import (
	"encoding/json"
	"fmt"
	"time"
)

// ES不同版本的bulk, 只和meta参数有关, 加上typename的判定即可
func (c *BulkClient) getMetaData(indexname, typename string) string {
	ret := map[string]map[string]string{
		"index": {
			"_index": indexname,
		},
	}

	if typename != "" {
		ret["index"]["_type"] = typename
	}

	res, _ := json.Marshal(ret)
	return string(res)
}

// Add 插入一条记录
func (c *BulkClient) Add(indexname, typename, message string) (int, time.Duration) {
	metadata := c.getMetaData(indexname, typename)

	c.datas.WriteString(fmt.Sprintf("%s\n", metadata))
	c.datas.WriteString(fmt.Sprintf("%s\n", message))
	// 新增原始数据数组, 当出现bulk异常时, 打印原始数据
	// 对于es_rejected_execution_exception的, 尝试重新提交一次, 失败的不再提交
	c.rawdatas = append(c.rawdatas, map[string]string{
		"meta": metadata,
		"msg":  message,
	})
	// 自增, 单线程不需要加锁
	c.ct++
	c.size += len(metadata) + len(message)

	if c.ct > c.maxCT {
		c.logger.Debug(fmt.Sprintf("线程号[%d], 超过最大条目数限制, 共提交%d字节", c.tid, c.size))
		return c.send()
	}

	if c.size > c.maxSize {
		c.logger.Debug(fmt.Sprintf("线程号[%d], 超过最大字节限制, 共提交%d条记录", c.tid, c.ct))
		return c.send()
	}

	return 0, 0
}

// Tick 提供外部Ticker调用
func (c *BulkClient) Tick() (int, time.Duration) {
	if c.ct > 0 && time.Since(c.lastTime) > c.maxTime {
		c.logger.Debug(fmt.Sprintf("线程号[%d], 超过最大时间限制, 共提交%d条记录", c.tid, c.ct))
		return c.send()
	}

	return 0, 0
}

// Send 当调用方推出循环后, 如果缓冲中仍然有数据的, 调用该方法完成发送
func (c *BulkClient) Send() (int, time.Duration) {
	if c.ct > 0 {
		c.logger.Info(fmt.Sprintf("线程号[%d], 将剩余的[%d]条数据发送", c.tid, c.ct))
		return c.send()
	}

	return 0, 0
}

func (c *BulkClient) Stop() {
	c.running = false
}
