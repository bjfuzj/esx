package esx

import (
	"encoding/json"
	"fmt"
	"time"
)

func (c *BulkClient) reset() {
	c.ct = 0
	c.size = 0
	c.lastTime = time.Now()

	c.datas.Reset()
	c.rawdatas = make([]map[string]string, 0)
}

func (c *BulkClient) checkResp(rdata []byte) bool {
	var err error
	var resp bulkRespTemp

	err = json.Unmarshal(rdata, &resp)
	if err != nil {
		c.logger.Warn(fmt.Sprintf("线程号[%d], bulk响应反序列化失败: %s", c.tid, rdata))
		return false
	}

	ss := false
	if resp.Errors {
		c.logger.Error(fmt.Sprintf("线程号[%d], bulk发生错误", c.tid))
		for idx, x := range resp.Iterms {
			if x.Index.RetInfo.Reason != "" {
				if x.Index.RetInfo.Type == ES_REJECTED_ERROR {
					c.logger.Warn(fmt.Sprintf("原始数据: %s, 错误原因: %s", c.rawdatas[idx]["msg"], x.Index.RetInfo.Reason))
					if !ss {
						c.datas.Reset()
						ss = true
					}
					c.datas.WriteString(fmt.Sprintf("%s\n", c.rawdatas[idx]["meta"]))
					c.datas.WriteString(fmt.Sprintf("%s\n", c.rawdatas[idx]["msg"]))
				} else {
					c.logger.Warn(fmt.Sprintf("原始数据: %s, 错误原因: %s, 引发: %s", c.rawdatas[idx]["msg"], x.Index.RetInfo.Reason, x.Index.RetInfo.CausedBy["reason"]))
				}
			}
		}
	}

	if ss {
		// 重新发起一次提交, 再失败则不记录了
		err = c.resend()
		if err != nil {
			c.logger.Warn(err.Error())
			return false
		}

		return true
	}

	return true
}
