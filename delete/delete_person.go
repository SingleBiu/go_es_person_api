/*
 * @Author: SingleBiu
 * @Date: 2025-10-26 20:12:57
 * @LastEditors: SingleBiu
 * @LastEditTime: 2025-10-26 20:13:02
 * @Description: file content
 */
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

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

	// 3. 示例1：根据 ID 删除单条数据（最常用）
	fmt.Println("--- 根据 ID 删除单条数据 ---")
	deleteID := "p1001" // 要删除的文档 ID
	if err := deleteByID(es, indexName, deleteID); err != nil {
		log.Printf("删除失败: %v", err)
	} else {
		fmt.Printf("ID 为 %s 的数据已成功删除\n", deleteID)
	}

	// 4. 示例2：删除整个索引（谨慎！会删除所有数据）
	// 注意：此操作会删除索引及所有数据，生产环境慎用！
	// fmt.Println("\n--- 删除整个索引 ---")
	// if err := deleteIndex(es, indexName); err != nil {
	// 	log.Printf("删除索引失败: %v", err)
	// } else {
	// 	fmt.Printf("索引 '%s' 已完全删除（包含所有数据）\n", indexName)
	// }
}

// 根据 ID 删除单条数据
func deleteByID(es *elasticsearch.Client, indexName, id string) error {
	// 创建删除请求（指定索引和文档 ID）
	req := esapi.DeleteRequest{
		Index:      indexName,
		DocumentID: id,     // 要删除的文档 ID
		Refresh:    "true", // 立即刷新（测试用）
	}

	// 执行请求
	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("执行删除请求失败: %v", err)
	}
	defer res.Body.Close()

	// 处理响应（404 表示文档不存在）
	if res.StatusCode == 404 {
		return fmt.Errorf("ID 为 %s 的数据不存在", id)
	}
	if res.IsError() {
		return fmt.Errorf("删除失败，状态: %s", res.Status())
	}
	return nil
}

// 删除整个索引（包含所有数据，谨慎使用）
func deleteIndex(es *elasticsearch.Client, indexName string) error {
	// 创建删除索引请求
	req := esapi.IndicesDeleteRequest{
		Index: []string{indexName}, // 要删除的索引名
	}

	// 执行请求
	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("执行删除索引请求失败: %v", err)
	}
	defer res.Body.Close()

	// 处理响应（404 表示索引不存在）
	if res.StatusCode == 404 {
		return fmt.Errorf("索引 '%s' 不存在", indexName)
	}
	if res.IsError() {
		return fmt.Errorf("删除索引失败，状态: %s", res.Status())
	}
	return nil
}
