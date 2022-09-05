package esx_search

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bjfuzj/esx"
)

func Search(client *esx.Client, in EasyRequest) (SearchResult, error) {
	var err error
	req, err := NewSearchRequest(&in)
	if err != nil {
		return SearchResult{}, err
	}

	reqbody := req.Tostring()
	uri := fmt.Sprintf("%s/_search?rest_total_hits_as_int=true&ignore_unavailable=true", strings.Join(req.Indices, ","))

	code, rdata := client.GetResponse(
		"GET", uri, reqbody, map[string]string{"Content-Type": "application/json"})
	if code >= 400 {
		return SearchResult{}, errors.New("请求ES失败")
	}

	return ParseSearchResult(rdata, &in)
}

func SearchWithPool(in EasyRequest) (SearchResult, error) {
	var err error
	req, err := NewSearchRequest(&in)
	if err != nil {
		return SearchResult{}, err
	}

	reqbody := req.Tostring()
	uri := fmt.Sprintf("%s/_search?rest_total_hits_as_int=true&ignore_unavailable=true", strings.Join(req.Indices, ","))

	// client, err := esx.Pool.GetTimeout(10 * time.Second)
	// if client != nil {
	// 	defer esx.Pool.Put(client)
	// }
	// if err != nil {
	// 	return SearchResult{}, err
	// }

	client := esx.Pool.Get()
	if client != nil {
		defer esx.Pool.Put(client)
	}

	code, rdata := client.GetResponse(
		"GET", uri, reqbody, map[string]string{"Content-Type": "application/json"})
	if code >= 400 {
		return SearchResult{}, errors.New("请求ES失败")
	}

	return ParseSearchResult(rdata, &in)
}

// TODO MultiSearch
