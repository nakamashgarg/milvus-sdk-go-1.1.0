package main

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

func deleteToMilvus(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	defer wg.Done()
	defer measureTime("deleteFromMilvus")()
	var i int64
	println("inside delete record function")
	for i = 0; i < 20; i++ {
		//println("delete %d record", i)
		status, err := client.DeleteEntityByID(ctx, collectionName, "", []int64{i})
		if err != nil {
			println("delete vector failed: " + err.Error())
		}
		if !status.Ok() {
			println("Delete vector failed: " + status.GetMessage())
		}
		//println("deleted %d record", i)
	}
}
