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

// Person 定义数据结构（与索引字段对应）
type Person struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// 搜索结果结构体（用于解析 Elasticsearch 响应）
type SearchResult struct {
	Hits struct {
		Hits []struct {
			Source Person `json:"_source"` // 实际的文档数据
		} `json:"hits"`
	} `json:"hits"`
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

	// 3. 示例1：根据 ID 精确查询（最常用）
	fmt.Println("--- 根据 ID 查询 ---")
	person, err := queryByID(es, indexName, "p1001")
	if err != nil {
		log.Printf("ID 查询失败: %v", err)
	} else if person.ID != "" {
		fmt.Printf("找到数据: ID=%s, Name=%s\n", person.ID, person.Name)
	} else {
		fmt.Println("未找到对应 ID 的数据")
	}

	// 4. 示例2：根据姓名模糊查询（支持分词匹配）
	fmt.Println("\n--- 根据姓名模糊查询 ---")
	persons, err := queryByName(es, indexName, "张") // 搜索姓名包含"张"的人
	if err != nil {
		log.Printf("姓名查询失败: %v", err)
	} else {
		fmt.Printf("找到 %d 条结果:\n", len(persons))
		for _, p := range persons {
			fmt.Printf("ID=%s, Name=%s\n", p.ID, p.Name)
		}
	}

	// 5. 示例3：查询所有数据（适合数据量小时使用）
	fmt.Println("\n--- 查询所有数据 ---")
	allPersons, err := queryAll(es, indexName)
	if err != nil {
		log.Printf("查询所有数据失败: %v", err)
	} else {
		fmt.Printf("共 %d 条数据:\n", len(allPersons))
		for _, p := range allPersons {
			fmt.Printf("ID=%s, Name=%s\n", p.ID, p.Name)
		}
	}
}

// 根据 ID 精确查询（效率最高）
func queryByID(es *elasticsearch.Client, indexName, id string) (Person, error) {
	var person Person

	// 创建查询请求（根据文档 ID 直接获取）
	req := esapi.GetRequest{
		Index:      indexName,
		DocumentID: id,
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return person, fmt.Errorf("执行查询失败: %v", err)
	}
	defer res.Body.Close()

	// 处理响应（404 表示未找到）
	if res.StatusCode == 404 {
		return person, nil // 返回空对象
	}
	if res.IsError() {
		return person, fmt.Errorf("查询错误: %s", res.Status())
	}

	// 解析响应结果
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return person, fmt.Errorf("解析结果失败: %v", err)
	}

	// 提取 _source 中的数据（即文档内容）
	source := result["_source"].(map[string]interface{})
	person.ID = source["id"].(string)
	person.Name = source["name"].(string)

	return person, nil
}

// 根据姓名模糊查询（支持分词，如搜索"张"匹配"张三"）
func queryByName(es *elasticsearch.Client, indexName, name string) ([]Person, error) {
	var persons []Person

	// 构建查询条件（匹配 name 字段，使用分词后的结果）
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"name": name, // 搜索 name 字段包含指定关键词的文档
			},
		},
	}
	queryBytes, _ := json.Marshal(query)

	// 创建搜索请求
	req := esapi.SearchRequest{
		Index: []string{indexName},
		Body:  strings.NewReader(string(queryBytes)),
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return persons, fmt.Errorf("执行搜索失败: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return persons, fmt.Errorf("搜索错误: %s", res.Status())
	}

	// 解析搜索结果
	var result SearchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return persons, fmt.Errorf("解析结果失败: %v", err)
	}

	// 提取所有命中的文档
	for _, hit := range result.Hits.Hits {
		persons = append(persons, hit.Source)
	}

	return persons, nil
}

// 查询所有数据（数据量大时慎用，建议分页）
func queryAll(es *elasticsearch.Client, indexName string) ([]Person, error) {
	var persons []Person

	// 构建查询条件（match_all 表示匹配所有文档）
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	queryBytes, _ := json.Marshal(query)

	// 创建搜索请求
	req := esapi.SearchRequest{
		Index: []string{indexName},
		Body:  strings.NewReader(string(queryBytes)),
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return persons, fmt.Errorf("执行搜索失败: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return persons, fmt.Errorf("搜索错误: %s", res.Status())
	}

	// 解析结果（与模糊查询逻辑相同）
	var result SearchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return persons, fmt.Errorf("解析结果失败: %v", err)
	}

	for _, hit := range result.Hits.Hits {
		persons = append(persons, hit.Source)
	}

	return persons, nil
}
