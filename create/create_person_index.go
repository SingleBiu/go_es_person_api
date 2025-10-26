/*
 * @Author: SingleBiu
 * @Date: 2025-10-26 19:57:02
 * @LastEditors: SingleBiu
 * @LastEditTime: 2025-10-26 19:57:08
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

// 索引映射配置：专门存储姓名和ID
const indexMapping = `
{
  "settings": {
    "number_of_shards": 1,    // 数据量较小时1个主分片足够
    "number_of_replicas": 0   // 测试环境可关闭副本,生产环境建议设为1
  },
  "mappings": {
    "properties": {
      "id": {
        "type": "keyword"  // ID通常用于精确匹配 如查询、更新 ,用keyword类型
      },
      "name": {
        "type": "text",    // 姓名需要支持分词查询 如搜索"张"能找到"张三" 
        "analyzer": "ik_max_word",  // 中文分词 需安装IK分词器 
        "fields": {
          "keyword": {
            "type": "keyword"  // 子字段用于精确匹配 如按全名筛选 
          }
        }
      }
    }
  }
}
`

func main() {
	// 1. 配置Elasticsearch连接
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200", // 替换为你的ES地址
		},
		// 如有认证,添加：
		// Username: "elastic",
		// Password: "你的密码",
	}

	// 2. 初始化客户端
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("初始化ES客户端失败: %v", err)
	}

	// 3. 定义索引名 类似表名,这里用"person"存储人员信息
	indexName := "person"

	// 4. 检查索引是否已存在
	existsReq := esapi.IndicesExistsRequest{Index: []string{indexName}}
	existsRes, err := existsReq.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("检查索引是否存在失败: %v", err)
	}
	defer existsRes.Body.Close()

	if existsRes.StatusCode == 200 {
		log.Fatalf("索引 '%s' 已存在,无需重复创建", indexName)
	} else if existsRes.StatusCode != 404 {
		log.Fatalf("检查索引状态异常: %s", existsRes.Status())
	}

	// 5. 创建索引 应用映射配置
	createReq := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(indexMapping),
	}
	createRes, err := createReq.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("创建索引失败: %v", err)
	}
	defer createRes.Body.Close()

	// 6. 解析并输出结果
	var result map[string]interface{}
	if err := json.NewDecoder(createRes.Body).Decode(&result); err != nil {
		log.Fatalf("解析响应失败: %v", err)
	}

	if createRes.IsError() {
		log.Fatalf("创建失败: %s", result["error"].(map[string]interface{})["reason"])
	}
	fmt.Printf("索引 '%s' 创建成功！\n", indexName)
	fmt.Printf("响应: %+v\n", result)
}
