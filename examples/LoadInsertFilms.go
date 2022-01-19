package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

/*
   search records by id's
*/
func searchEntityById(ctx context.Context, client milvus.MilvusClient, searchId []int64) []milvus.Entity {
	//	defer measureTime("searchFromMilvus")()
	entity, status, err := client.GetEntityByID(ctx, collectionName, "", searchId)
	//println("***************************")
	//println("%+v", entity)
	//println(len(entity))
	//println("%+v", status.GetStatus().GetMessage())

	//println("***************************")
	if err != nil {
		println("Search rpc failed: " + err.Error())
		return entity
	}
	if !status.Ok() {
		println("Search vector failed: " + status.GetMessage())
		return entity
	}
	return entity
}

func insertSingleFilm(ctx context.Context, client milvus.MilvusClient, film film, wg *sync.WaitGroup) {
	defer wg.Done()
	//defer measureTime("insertion of single record")()
	vectors := make([][]float32, 0, 1)
	records := make([]milvus.Entity, 1)
	entity := searchEntityById(ctx, client, []int64{film.ID})
	//println("entityId", entity)
	if len(entity) != 0 && len(entity[0].FloatData) != 0 {
		return
	}
	vectors = append(vectors, film.Vector[:]) // prevent same vector
	//	println(film.ID, "          ", film.Vector[:])
	records[0].FloatData = vectors[0]
	insertParam := milvus.InsertParam{collectionName, "", records, []int64{film.ID}}
	_, status, err := client.Insert(ctx, &insertParam)
	if err != nil {
		println("Insert rpc failed: " + err.Error())
		return
	}
	if !status.Ok() {
		println("Insert vector failed: " + status.GetMessage())
		return
	}
	//println("Insert vectors success!")
}

/*
  insert films data into milvus
*/
func insertFilms(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	//defer measureTime("total time to insert 1500 records")()
	defer wg.Done()
	var i int64
	var wg1 sync.WaitGroup
	for i = 0; i < 1500; i++ {
		wg1.Add(1)
		insertSingleFilm(ctx, client, films[i], &wg1)
	}
	wg1.Wait()

}

func deleteSingleFilm(ctx context.Context, client milvus.MilvusClient, id_array []int64, wg *sync.WaitGroup) {
	//defer measureTime("deletion of single record")()
	defer wg.Done()
	status, err := client.DeleteEntityByID(ctx, collectionName, "", id_array)
	if err != nil {
		println("delete vector failed: " + err.Error())
	}
	if !status.Ok() {
		println("Delete vector failed: " + status.GetMessage())
	}
	//println("delete vector success")
}

func deleteFilms(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	//defer measureTime("==============**********total time to delete 1500 records is**********============")()
	defer wg.Done()
	var i int64
	var wg1 sync.WaitGroup
	for i = 0; i < 1500; i++ {
		wg1.Add(1)
		deleteSingleFilm(ctx, client, []int64{i}, &wg1)
	}
	wg1.Wait()
}

func searchBySingleVector(ctx context.Context, client milvus.MilvusClient, records []milvus.Entity, wg *sync.WaitGroup) {

	start := time.Now()
	defer wg.Done()
	//var topkQueryResult milvus.TopkQueryResult
	extraParams := "{\"nprobe\" : 32}"
	searchParam := milvus.SearchParam{collectionName, records, topk, nil, extraParams}
	_, status, err = client.Search(ctx, searchParam)
	if err != nil {
		println("Search rpc failed: " + err.Error())
	}
	fmt.Printf("Time taken to %s is %v \n", "search single record", time.Since(start))
	//println("total records", len(topkQueryResult.QueryResultList[0].Distances))
}

/*
   search milvus data by vector
*/
func searchByVector(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	//defer measureTime("==============**********time taken to search 300 records ***************===============")()
	defer wg.Done()
	start := time.Now()
	var i int64
	//Construct query vectors
	var wg1 sync.WaitGroup
	for i = 2000; i < 2300; i++ {
		wg1.Add(1)
		vectors := make([][]float32, 0, 1)
		records := make([]milvus.Entity, 1)
		vectors = append(vectors, films[i].Vector[:]) // prevent same vector
		//	println(films[i].Vector[:])
		records[0].FloatData = vectors[0]
		searchBySingleVector(ctx, client, records, &wg1)
	}
	wg1.Wait()
	fmt.Printf("Time taken to %s is %v \n", "search 300 records", time.Since(start))
}

type film struct {
	ID     int64
	Title  string
	Year   int32
	Vector [8]float32 // fix length array
}

func loadFilmCSV() ([]film, error) {
	f, err := os.Open("films.csv")
	if err != nil {
		return []film{}, err
	}
	r := csv.NewReader(f)
	raw, err := r.ReadAll()
	if err != nil {
		return []film{}, err
	}
	films := make([]film, 0, len(raw))
	for _, line := range raw {
		if len(line) < 4 { // insuffcient column
			continue
		}
		fi := film{}
		// ID
		v, err := strconv.ParseInt(line[0], 10, 64)
		if err != nil {
			continue
		}
		fi.ID = v
		// Title
		fi.Title = line[1]
		// Year
		v, err = strconv.ParseInt(line[2], 10, 64)
		if err != nil {
			continue
		}
		fi.Year = int32(v)
		// Vector
		vectorStr := strings.ReplaceAll(line[3], "[", "")
		vectorStr = strings.ReplaceAll(vectorStr, "]", "")
		parts := strings.Split(vectorStr, ",")
		if len(parts) != 8 { // dim must be 8
			continue
		}
		for idx, part := range parts {
			part = strings.TrimSpace(part)
			v, err := strconv.ParseFloat(part, 32)
			if err != nil {
				continue
			}
			fi.Vector[idx] = float32(v)
		}
		films = append(films, fi)
	}
	return films, nil
}

func insertAllFilms(ctx context.Context, client milvus.MilvusClient) {
	records := make([]milvus.Entity, len(films))
	vectors := make([][]float32, len(films))
	ids := make([]int64, 0, len(films))
	for idx, film := range films {
		vectors[idx] = make([]float32, 8)
		//println(film.Vector)
		ids = append(ids, film.ID)
		vectors = append(vectors, film.Vector[:])
		records[idx].FloatData = vectors[idx]
	}
	insertParam := milvus.InsertParam{collectionName, "", records, ids}
	_, status, err = client.Insert(ctx, &insertParam)
	if err != nil {
		println("delete vector failed: " + err.Error())
	}
	if !status.Ok() {
		println("Delete vector failed: " + status.GetMessage())
	}

}
