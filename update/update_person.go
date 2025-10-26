/*
 * @Author: SingleBiu
 * @Date: 2025-10-26 20:10:56
 * @LastEditors: SingleBiu
 * @LastEditTime: 2025-10-26 20:11:01
 * @Description: file content
 */
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

// Person 数据结构
type Person struct {
	ID   string `json:"id"`
	Name string `json:"name"`
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

	indexName := "person"

	// 3. 示例1：全量更新（覆盖整个文档）
	fmt.Println("--- 全量更新 ---")
	// 假设更新 ID 为 p1001 的数据（全量替换）
	fullUpdatePerson := Person{
		ID:   "p1001", // 必须指定要更新的 ID
		Name: "张三三",   // 新的姓名
	}
	if err := fullUpdate(es, indexName, fullUpdatePerson); err != nil {
		log.Printf("全量更新失败: %v", err)
	} else {
		fmt.Println("全量更新成功")
	}

	// 4. 示例2：部分更新（仅修改指定字段）
	fmt.Println("\n--- 部分更新 ---")
	// 仅更新 ID 为 p1002 的姓名，其他字段不变
	if err := partialUpdate(es, indexName, "p1002", "李思思"); err != nil {
		log.Printf("部分更新失败: %v", err)
	} else {
		fmt.Println("部分更新成功")
	}
}

// 全量更新：用新的文档覆盖旧文档（需提供完整字段）
func fullUpdate(es *elasticsearch.Client, indexName string, person Person) error {
	// 序列化完整文档
	docBytes, err := json.Marshal(person)
	if err != nil {
		return fmt.Errorf("序列化数据失败: %v", err)
	}

	// 创建索引请求（ID 存在则更新，不存在则插入）
	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: person.ID, // 指定要更新的文档 ID
		Body:       strings.NewReader(string(docBytes)),
		Refresh:    "true", // 立即刷新（测试用）
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("执行更新请求失败: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("更新失败，状态: %s", res.Status())
	}
	return nil
}

// 部分更新：仅修改指定字段（无需提供完整文档）
func partialUpdate(es *elasticsearch.Client, indexName, id, newName string) error {
	// 构建部分更新请求体（仅包含要修改的字段）
	updateBody := map[string]interface{}{
		"doc": map[string]interface{}{ // "doc" 表示要更新的字段
			"name": newName, // 仅更新 name 字段
		},
	}
	bodyBytes, err := json.Marshal(updateBody)
	if err != nil {
		return fmt.Errorf("构建更新体失败: %v", err)
	}

	// 创建更新请求
	req := esapi.UpdateRequest{
		Index:      indexName,
		DocumentID: id, // 指定要更新的文档 ID
		Body:       strings.NewReader(string(bodyBytes)),
		Refresh:    "true", // 立即刷新（测试用）
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("执行部分更新失败: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("部分更新失败，状态: %s", res.Status())
	}
	return nil
}
