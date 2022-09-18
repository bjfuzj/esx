package esx

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

func formatAddrs(addrs []string) []string {
	ret := make([]string, 0)
	for _, x := range addrs {
		if strings.HasPrefix(x, "http://") || strings.HasPrefix(x, "https://") {
			ret = append(ret, x)
		} else {
			ret = append(ret, fmt.Sprintf("http://%s", x))
		}
	}

	return ret
}

func NewClient(logger *zap.Logger, opt Option) (*Client, error) {
	if len(opt.Addrs) == 0 {
		return nil, errors.New("ES地址列表为空")
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}
	if opt.Username != "" && opt.Password != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", opt.Username, opt.Password)))
		headers["Authorization"] = fmt.Sprintf("Basic %s", auth)
	}

	addrs := formatAddrs(opt.Addrs)

	return &Client{
		tid:    0,
		logger: logger,
		addrs:  addrs,
		client: &http.Client{
			Timeout: opt.Timeout,
		},
		defaultHeaders: headers,
	}, nil
}

func NewClientWithID(logger *zap.Logger, opt Option, tid int) (*Client, error) {
	if len(opt.Addrs) == 0 {
		return nil, errors.New("ES地址列表为空")
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}
	if opt.Username != "" && opt.Password != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", opt.Username, opt.Password)))
		headers["Authorization"] = fmt.Sprintf("Basic %s", auth)
	}

	addrs := formatAddrs(opt.Addrs)

	return &Client{
		tid:    tid,
		logger: logger,
		addrs:  addrs,
		client: &http.Client{
			Timeout: opt.Timeout,
		},
		defaultHeaders: headers,
	}, nil
}

func NewPool(logger *zap.Logger, opt Option, size int) error {
	if len(opt.Addrs) == 0 {
		return errors.New("ES地址列表为空")
	}

	if Pool != nil {
		return nil
	}

	Pool = &ClientPool{
		logger: logger,
		pool:   make([]*Client, size),
		queue:  make(chan int, size),
	}

	for x := 0; x < size; x++ {
		Pool.queue <- x
		client, _ := NewClientWithID(logger, opt, x)
		Pool.pool[x] = client
	}

	return nil
}

func (c *Client) geturl(uri string) string {
	ret := fmt.Sprintf("%s/%s", c.addrs[c.node], uri)
	c.node = (c.node + 1) % len(c.addrs)

	// 2022-09-18 改成在初始化的时候判定添加, 使用 formatAddrs 函数
	// if !strings.HasPrefix(ret, "http://") && !strings.HasPrefix(ret, "https://") {
	// 	ret = fmt.Sprintf("http://%s", ret)
	// }

	return ret
}

func (c *Client) GetResponse(method, uri, data string, headers map[string]string) (int, []byte) {
	c.logger.Debug(fmt.Sprintf("请求报文: %s", data))

	req, _ := http.NewRequest(method, c.geturl(uri), strings.NewReader(data))
	for k, v := range c.defaultHeaders {
		req.Header.Set(k, v)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		c.logger.Warn(fmt.Sprintf("%s %s 操作失败: %s", method, uri, err.Error()))
		return HTTP_REQ_FAIL, nil
	}

	rdata, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.logger.Warn(fmt.Sprintf("%s %s 读取response失败: %s", method, uri, err.Error()))
		return HTTP_RESP_READ_FAIL, nil
	}

	c.logger.Debug(fmt.Sprintf("待解析的原始响应报文: %s", string(rdata)))
	return resp.StatusCode, rdata
}

func (c *ClientPool) Get() *Client {
	// 必须拿到为止
	tid := <-c.queue
	return c.pool[tid]
}

func (c *ClientPool) GetTimeout(t time.Duration) (*Client, error) {
	tm := time.NewTimer(t)
	select {
	case tid := <-c.queue:
		return c.pool[tid], nil
	case <-tm.C:
		return nil, errors.New("从ES连接池获取链接超时")
	}
}

func (c *ClientPool) Put(client *Client) {
	// 链接指针一直在数组中
	// 只是将索引小标规划到队列
	c.queue <- client.tid
}
