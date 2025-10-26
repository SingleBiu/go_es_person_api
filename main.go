package main

import (
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
)

func main() {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("创建客户端失败: %v", err)
	}

	// 直接调用 Info 方法，无需显式传递上下文（内部默认使用 context.Background()）
	res, err := es.Info()
	if err != nil {
		log.Fatalf("请求失败: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("响应错误: %s", res.Status())
	}

	fmt.Println("Elasticsearch 连接成功！状态:", res.Status())
}
