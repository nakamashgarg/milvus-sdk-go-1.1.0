/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

var collectionName string = "test_go2"
var dimension int64 = 128
var indexFileSize int64 = 1024
var metricType int32 = int32(milvus.L2)
var nq int64 = 1000
var nprobe int64 = 64
var nb int64 = 100000
var topk int64 = 100
var nlist int64 = 16384
var err error
var status milvus.Status

/**
  measure function time by just calling this
**/
func measureTime(funcName string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("Time taken by %s function is %v \n", funcName, time.Since(start))
	}
}

/**
  build milvus connection
**/
func serverConnection(address string, port string) (context.Context, milvus.MilvusClient) {
	connectParam := milvus.ConnectParam{IPAddress: address, Port: port}
	ctx := context.TODO()
	client, err := milvus.NewMilvusClient(ctx, connectParam)
	if err != nil {
		log.Fatalf("Client connect failed: %v", err)
	}

	//Client version
	println("Client version: " + client.GetClientVersion(ctx))

	if client.IsConnected(ctx) == false {
		println("client: not connected: ")
	}
	println("Server status: connected")

	println("**************************************************")
	return ctx, client
}

/**
  create collection in milvus
**/
func createCollection(ctx context.Context, client milvus.MilvusClient) {
	//Get server version
	var version string
	version, status, err = client.ServerVersion(ctx)
	if err != nil {
		println("Cmd rpc failed: " + err.Error())
	}
	if !status.Ok() {
		println("Get server version failed: " + status.GetMessage())
		return
	}
	println("Server version: " + version)
	collectionParam := milvus.CollectionParam{collectionName, dimension, indexFileSize, metricType}
	hasCollection, status, err := client.HasCollection(ctx, collectionName)
	if err != nil {
		println("HasCollection rpc failed: " + err.Error())
	}
	if hasCollection == false {
		status, err = client.CreateCollection(ctx, collectionParam)
		if err != nil {
			println("CreateCollection rpc failed: " + err.Error())
			return
		}
		if !status.Ok() {
			println("Create collection failed: " + status.GetMessage())
			return
		}
		println("Create collection " + collectionName + " success")
	}

	hasCollection, status, err = client.HasCollection(ctx, collectionName)
	if err != nil {
		println("HasCollection rpc failed: " + err.Error())
		return
	}
	if hasCollection == false {
		println("Create collection failed: " + status.GetMessage())
		return
	}
	println("Collection: " + collectionName + " exist")
}

/**
  insert data in colection
**/
func insertToMilvus(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	defer wg.Done()
	var i int64
	var collections []string
	collections, status, err = client.ListCollections(ctx)
	if err != nil {
		println("ShowCollections rpc failed: " + err.Error())
		return
	}
	if !status.Ok() {
		println("Show collections failed: " + status.GetMessage())
		return
	}
	println("ShowCollections: ")
	for i = 0; i < int64(len(collections)); i++ {
		println(" - " + collections[i])
	}
	println("**************************************************")

	var loop bool
	loop = true
	for loop == true {
		//insert(ctx, client)
	}
}

func insert(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	defer wg.Done()
	defer measureTime("insertToMilvus")()
	var i, j int64
	records := make([]milvus.Entity, nb)
	recordArray := make([][]float32, nb)
	vector_ids := makeRange(0, nb)
	for i = 0; i < nb; i++ {
		recordArray[i] = make([]float32, dimension)
		for j = 0; j < dimension; j++ {
			recordArray[i][j] = float32(i % (j + 1))
		}
		records[i].FloatData = recordArray[i]
	}
	insertParam := milvus.InsertParam{collectionName, "", records, vector_ids}
	_, status, err := client.Insert(ctx, &insertParam)

	if err != nil {
		println("Insert rpc failed: " + err.Error())
		return
	}
	if !status.Ok() {
		println("Insert vector failed: " + status.GetMessage())
		return
	}
	println("Insert vectors success!")
}

func makeRange(min, max int64) []int64 {
	a := make([]int64, max-min)
	var i int64

	for i = 0; i < int64(len(a)); i++ {
		a[i] = min + i
	}
	return a
}

/**
  drop collection
**/
func dropCollection(ctx context.Context, client milvus.MilvusClient) {
	status, err = client.DropCollection(ctx, collectionName)
	hasCollection, status1, err := client.HasCollection(ctx, collectionName)
	if !status.Ok() || !status1.Ok() || hasCollection == true {
		println("Drop collection failed: " + status.GetMessage())
		return
	}
	println("Drop collection " + collectionName + " success!")

	//GetConfig
	var configInfo string
	configInfo, status, err = client.GetConfig(ctx, "*")
	if !status.Ok() {
		println("Get config failed: " + status.GetMessage())
	}
	println("config: ")
	println(configInfo)

	//Disconnect
	err = client.Disconnect(ctx)
	if err != nil {
		println("Disconnect failed!")
		return
	}
	println("Client disconnect server success!")

	//Server status
	var serverStatus string
	serverStatus, status, err = client.ServerStatus(ctx)
	if !status.Ok() {
		println("Get server status failed: " + status.GetMessage())
	}
	println("Server status: " + serverStatus)
}

func main() {
	//	var wg sync.WaitGroup
	address := "10.21.33.1"
	port := "19530"
	var ctx, client = serverConnection(address, port)
	//createCollection(ctx, client)
	//searchEntityByVector(ctx, client)
	//searchEntityById(ctx, client)
	//var loop bool
	//loop = true
	// for loop == true {
	// 	wg.Add(3)
	// 	insert(ctx, client, &wg)
	// 	delete(ctx, client, &wg)
	// 	searchEntityByVector(ctx, client, &wg)
	// 	wg.Wait()
	// }
	countEntities(ctx, client)
	println("**************************************************")
	//dropCollection(ctx, client)
}
