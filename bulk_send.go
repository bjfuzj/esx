package esx

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (c *BulkClient) send() (int, time.Duration) {
	// 提交请求失败的, 一直循环
	// 请求成功后, 则只校验是否发生rejected
	// 只有es_rejected_execution_exception的会重试一次
	headers := map[string]string{
		"Content-Type": "application/x-ndjson",
	}
	datas := c.datas.String()

	ss := false
	timeFrom := time.Now()
	for c.running {
		code, rdata := c.client.GetResponse("POST", "_bulk", datas, headers)
		if code > 200 {
			c.logger.Warn(fmt.Sprintf("线程号[%d], http_code = %d, 响应报文: %s", c.tid, code, string(rdata)))
			time.Sleep(time.Second)
			continue
		}

		// 校验并确认是否需要重试
		ss = c.checkResp(rdata)
		break
	}

	timeDelta := time.Since(timeFrom)
	ret := c.ct

	if ss {
		c.logger.Debug(fmt.Sprintf("线程号[%d], bulk完成, 耗时: %s", c.tid, timeDelta.String()))
	} else {
		c.logger.Warn(fmt.Sprintf("线程号[%d], bulk存在失败, 耗时: %s", c.tid, timeDelta.String()))
	}

	c.reset()

	return ret, timeDelta
}

func (c *BulkClient) resend() error {
	c.logger.Warn("触发resend")

	headers := map[string]string{
		"Content-Type": "application/x-ndjson",
	}
	datas := c.datas.String()

	code, rdata := c.client.GetResponse("POST", "_bulk", datas, headers)
	if code > 200 {
		return errors.New("resend请求失败")
	}

	var resp bulkRespTemp
	err := json.Unmarshal(rdata, &resp)
	if err != nil {
		return fmt.Errorf("resend响应结果解析失败: %s", err.Error())
	}

	if resp.Errors {
		return errors.New("resend仍然失败, 直接略过")
	}

	return nil
}
