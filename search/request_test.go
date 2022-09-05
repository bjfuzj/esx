package esx_search

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestBase64(t *testing.T) {
	a := base64.StdEncoding.EncodeToString([]byte("elastic:uPZ8m3GJ"))
	t.Log(a)
}

func TestAny(t *testing.T) {
	a := []string{"aa", "bb", "cc", "dd"}
	t.Log(a[5:8])
}

func TestAggs(t *testing.T) {
	a := `
	{
		"size": 123,
		"terms": ["abc", "edc"],
		"scripted_metric": {
			"toggle": true,
			"params": {
				"010": "总行",
				"110": "北京分行"
			},
			"init_script": "state.datas = [:]",
			"map_script": "",
			"combine_script": "return state.datas",
			"reduce_script": ""
		}
	}
	`

	var b EasyRequest
	json.Unmarshal([]byte(a), &b)

	c, err := NewSearchRequest(&b)
	if err != nil {
		t.Error(err.Error())
		return
	}

	t.Log(c.Tostring())
}
