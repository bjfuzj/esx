package esx

import (
	"bytes"
	"time"

	"go.uber.org/zap"
)

type bulkRespTemp struct {
	Errors bool `json:"errors"`
	Iterms []struct {
		Index struct {
			RetInfo struct {
				CausedBy map[string]string `json:"caused_by"`
				Reason   string            `json:"reason"`
				Type     string            `json:"type"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

type BulkOption struct {
	Tid     int
	MaxCT   int
	MaxSize int
	MaxTime time.Duration
}

type BulkClient struct {
	running bool
	logger  *zap.Logger
	tid     int
	client  *Client

	ct       int
	maxCT    int
	size     int
	maxSize  int
	lastTime time.Time
	maxTime  time.Duration

	datas    *bytes.Buffer
	rawdatas []map[string]string
}

func NewBulkClient(logger *zap.Logger, client *Client, opt BulkOption) *BulkClient {
	return &BulkClient{
		running: true,
		logger:  logger,
		tid:     opt.Tid,
		client:  client,

		ct:       0,
		maxCT:    opt.MaxCT,
		size:     0,
		maxSize:  opt.MaxSize,
		lastTime: time.Now(),
		maxTime:  opt.MaxTime,

		datas:    bytes.NewBufferString(""),
		rawdatas: make([]map[string]string, 0),
	}
}
