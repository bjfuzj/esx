package esx

import (
	"net/http"
	"time"

	"go.uber.org/zap"
)

// 关于连接池的实现
// ***实现方案一***
// 创建一个size大小的数组用于存放Client对象指针
// 再创建一个size大小的queue, 用于接收和取出数组的下标
// ***实现方案二***
// 同样是创建一个size大小的数组用于存放Client对象指针
// Client对象需要加锁, 以及上一次获取成功的时间戳
// 连接池需要加锁, 以及一个索引下标数值, 该数值Get时自增, Put时自减
// Get方法遍历数组, 判断获取时间戳是否超过Timeout的2倍

const (
	HTTP_SELF_CODE      = 900 // 自定义的http状态码边界
	HTTP_REQ_FAIL       = 901 // 请求发起就失败了, 不涉及响应
	HTTP_RESP_READ_FAIL = 902 // 响应数据读取失败

	ES_REJECTED_ERROR = "es_rejected_execution_exception"
)

var (
	Pool *ClientPool
)

type Option struct {
	Addrs    []string
	Username string
	Password string
	Timeout  time.Duration
}

type Client struct {
	tid    int // 提供给归还连接池时使用
	logger *zap.Logger

	addrs []string
	node  int // 当前使用节点下标

	client         *http.Client
	defaultHeaders map[string]string
}

type ClientPool struct {
	logger *zap.Logger
	pool   []*Client
	queue  chan int
}

type Ack struct {
	Acknowledged bool `json:"acknowledged"`
	Status       int  `json:"status"`
}
