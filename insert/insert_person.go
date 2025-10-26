package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// Person 定义数据结构（对应索引中的字段）
type Person struct {
	ID   string `json:"id"`   // 唯一标识
	Name string `json:"name"` // 姓名
}

func main() {
	// 1. 配置 Elasticsearch 连接
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
		// 认证信息（如有）
		// Username: "elastic",
		// Password: "your-password",
	}

	// 2. 初始化客户端
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("初始化客户端失败: %v", err)
	}

	// 3. 定义要操作的索引名（已创建的 "person" 索引）
	indexName := "person"

	// 4. 单条插入示例
	singlePerson := Person{
		ID:   "p1001", // 手动指定 ID（如用户ID、工号等）
		Name: "张三",
	}
	if err := insertSingle(es, indexName, singlePerson); err != nil {
		log.Printf("单条插入失败: %v", err)
	} else {
		fmt.Println("单条数据插入成功")
	}

	// 5. 批量插入示例（多条数据）
	batchPersons := []Person{
		{ID: "p1002", Name: "李四"},
		{ID: "p1003", Name: "王五"},
		{ID: "p1004", Name: "赵六"},
	}
	if err := insertBatch(es, indexName, batchPersons); err != nil {
		log.Printf("批量插入失败: %v", err)
	} else {
		fmt.Println("批量数据插入成功")
	}
}

// 单条插入数据
func insertSingle(es *elasticsearch.Client, indexName string, person Person) error {
	// 序列化数据为 JSON
	docBytes, err := json.Marshal(person)
	if err != nil {
		return fmt.Errorf("序列化数据失败: %v", err)
	}

	// 创建索引请求（指定文档 ID 为 person.ID）
	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: person.ID, // 用 person.ID 作为 Elasticsearch 文档的 _id
		Body:       strings.NewReader(string(docBytes)),
		Refresh:    "true", // 立即刷新索引（测试用，生产环境建议去掉）
	}

	// 执行请求
	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("执行插入请求失败: %v", err)
	}
	defer res.Body.Close()

	// 检查响应是否错误
	if res.IsError() {
		return fmt.Errorf("插入失败，状态: %s", res.Status())
	}
	return nil
}

// 批量插入数据（高效处理多条数据）
func insertBatch(es *elasticsearch.Client, indexName string, persons []Person) error {
	var bulkBody strings.Builder

	// 构建批量请求体（Elasticsearch Bulk API 格式）
	for _, p := range persons {
		// 1. 写入操作元数据（指定索引和文档 ID）
		meta := map[string]interface{}{
			"index": map[string]string{
				"_index": indexName,
				"_id":    p.ID, // 文档 ID 与 person.ID 一致
			},
		}
		metaBytes, _ := json.Marshal(meta)
		bulkBody.WriteString(string(metaBytes) + "\n")

		// 2. 写入文档数据
		docBytes, _ := json.Marshal(p)
		bulkBody.WriteString(string(docBytes) + "\n")
	}

	// 创建批量请求
	req := esapi.BulkRequest{
		Body:    strings.NewReader(bulkBody.String()),
		Refresh: "true", // 立即刷新（测试用）
	}

	// 执行批量请求
	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("执行批量请求失败: %v", err)
	}
	defer res.Body.Close()

	// 检查响应是否错误
	if res.IsError() {
		return fmt.Errorf("批量插入失败，状态: %s", res.Status())
	}
	return nil
}
