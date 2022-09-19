package esx

import (
	"testing"
	"time"

	"github.com/bjfuzj/esx/logx"
)

func TestCat(t *testing.T) {
	logger := logx.GetConsoleLogger("INFO")
	client, _ := NewClient(logger, Option{
		Addrs: []string{
			"tx04:8003",
		},
		Username: "elastic",
		Password: "uPZ8m3GJ",
		Timeout:  time.Minute,
	})

	result, err := Cat(client, "")
	if err != nil {
		t.Error(err.Error())
		return
	}

	for _, x := range result {
		t.Log(x.Index)
	}
}
