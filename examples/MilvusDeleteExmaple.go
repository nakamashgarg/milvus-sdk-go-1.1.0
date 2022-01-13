package main

import (
	"context"
	"math/rand"
	"sync"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

/*
   deleting records with indexes from milvus
*/
func deleteToMilvus(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	defer wg.Done()
	var loop bool
	loop = true
	for loop == true {
		//	delete(ctx, client)
	}
}

func delete(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	defer wg.Done()
	defer measureTime("deleteFromMilvus")()
	println("inside delete record function")
	vector_ids := makeRange(0, 1000)
	//println("delete %d record", i)
	status, err := client.DeleteEntityByID(ctx, collectionName, "", vector_ids)
	if err != nil {
		println("delete vector failed: " + err.Error())
	}
	if !status.Ok() {
		println("Delete vector failed: " + status.GetMessage())
	}
	println("message after deleting 1K records", status.GetMessage())
}

/*
   count number of present records in milvus
*/
func countEntities(ctx context.Context, client milvus.MilvusClient) {
	count, status, err := client.CountEntities(ctx, collectionName)
	if err != nil {
		println("Insert rpc failed: " + err.Error())
		return
	}
	if !status.Ok() {
		println("Insert vector failed: " + status.GetMessage())
		return
	}
	println("number of entities in collection is", count)
}

/*
   search records by id's
*/
func searchEntityById(ctx context.Context, client milvus.MilvusClient) {
	defer measureTime("searchFromMilvus")()
	vector_ids := makeRange(50000, 51000)
	println(len(vector_ids))
	entity, status, err := client.GetEntityByID(ctx, collectionName, "", vector_ids)
	println("***************************")
	println("%+v", entity)
	println(len(entity))
	println("%+v", status.GetStatus().GetMessage())

	println("***************************")
	if err != nil {
		println("Search rpc failed: " + err.Error())
		return
	}
	if !status.Ok() {
		println("Search vector failed: " + status.GetMessage())
		return
	}
	for _, elem := range entity {
		println("float****", elem.FloatData[0])
	}
}
func searchEntityByVector(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	defer wg.Done()
	defer measureTime("searchEntityByVector")()
	var i, j int64
	//Construct query vectors
	queryRecords := make([]milvus.Entity, nq)
	queryVectors := make([][]float32, nq)

	for i = 0; i < nq; i++ {
		queryVectors[i] = make([]float32, dimension)
		for j = 0; j < dimension; j++ {
			queryVectors[i][j] = float32(rand.Float64())
		}
		queryRecords[i].FloatData = queryVectors[i]
	}

	var topkQueryResult milvus.TopkQueryResult
	extraParams := "{\"nprobe\" : 32}"
	searchParam := milvus.SearchParam{collectionName, queryRecords, topk, nil, extraParams}
	topkQueryResult, status, err = client.Search(ctx, searchParam)
	if err != nil {
		println("Search rpc failed: " + err.Error())
	}
	println(topkQueryResult.QueryResultList[0].Distances[0])
	// println("Search without index results: ")
	// for i = 0; i < 100; i++ {
	// 	print(topkQueryResult.QueryResultList[i].Ids[0])
	// 	print("        ")
	// 	println(topkQueryResult.QueryResultList[i].Distances[0])
	// }

	println("**************************************************")
}
