package esx_search

import (
	"encoding/json"
	"testing"
)

func TestResponse(t *testing.T) {
	a := `
	{
		"size": 1,
		"terms": ["appname", "hostname"],
		"funcs": {
			"sum": ["amt"]
		}
	}
	`

	var ae EasyRequest
	_ = json.Unmarshal([]byte(a), &ae)

	ar, _ := NewSearchRequest(&ae)
	t.Log(ar.Tostring())

	b := `
	{
		"took" : 6,
		"timed_out" : false,
		"_shards" : {
		  "total" : 6,
		  "successful" : 6,
		  "skipped" : 0,
		  "failed" : 0
		},
		"hits" : {
		  "total" : 17280,
		  "max_score" : 1.0,
		  "hits" : [
			{
			  "_index" : "tpmsminute_test_test-2022.07.10",
			  "_type" : "_doc",
			  "_id" : "zpq-44EBGxhqP6RM51AE",
			  "_score" : 1.0,
			  "_source" : {
				"@timestamp" : 1657383240000,
				"ctime" : 1657383311254,
				"flag" : 0,
				"viewname" : "EC",
				"tenant" : "test",
				"envname" : "test",
				"platform" : "P0",
				"appid" : 52,
				"appname" : "测试系统1",
				"ec" : "000000000000",
				"viewkey" : "000000000000",
				"amt" : 173,
				"longamt" : 0,
				"longrate" : 0,
				"tapdex" : 17300,
				"apdex" : 100,
				"tssc" : 173,
				"ssc" : 100,
				"tbsc" : 173,
				"bsc" : 100,
				"tresptime" : 11094,
				"aresptime" : 64.13,
				"tproctime" : 2637,
				"aproctime" : 15.24,
				"money" : 8407.28
			  }
			}
		  ]
		},
		"aggregations" : {
		  "terms_appname" : {
			"doc_count_error_upper_bound" : 0,
			"sum_other_doc_count" : 0,
			"buckets" : [
			  {
				"key" : "测试系统1",
				"doc_count" : 17280,
				"terms_hostname" : {
				  "doc_count_error_upper_bound" : 0,
				  "sum_other_doc_count" : 0,
				  "buckets" : [
					{
					  "key" : "19ctestap01",
					  "doc_count" : 1440,
					  "sum_amt" : {
						"value" : 1353195.0
					  }
					}
				  ]
				}
			  }
			]
		  }
		}
	  }	  
	`

	res, err := ParseSearchResult([]byte(b), &ae)
	if err != nil {
		t.Error(err.Error())
		return
	}

	ret, _ := json.MarshalIndent(res, "", "  ")
	t.Log(string(ret))
}
