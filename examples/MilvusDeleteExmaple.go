package main

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

/*
   deleting records with indexes from milvus
*/
func deleteToMilvus(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
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
