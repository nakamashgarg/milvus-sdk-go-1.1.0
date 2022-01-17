package main

import (
	"context"
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

func insertSingleFilm(ctx context.Context, client milvus.MilvusClient, film film) {
	defer measureTime("insertion of single record")()
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
	println("Insert vectors success!")
}

/*
  insert films data into milvus
*/
func insertFilms(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	defer wg.Done()
	defer measureTime("total time to insert 1500 records")()
	films, err := loadFilmCSV()
	var i int64
	if err != nil {
		log.Fatal("failed to load film data csv:", err.Error())
	}
	for i = 1000; i < 2500; i++ {
		insertSingleFilm(ctx, client, films[i])
	}

}
func deleteSingleFilm(ctx context.Context, client milvus.MilvusClient, id_array []int64) {
	defer measureTime("deletion of single record")()
	status, err := client.DeleteEntityByID(ctx, collectionName, "", id_array)
	if err != nil {
		println("delete vector failed: " + err.Error())
	}
	if !status.Ok() {
		println("Delete vector failed: " + status.GetMessage())
	}
	println("delete vector success")
}

func deleteFilms(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	defer wg.Done()
	defer measureTime("==============**********total time to delete 1500 records is**********============")()
	var i int64
	for i = 1500; i < 3000; i++ {
		deleteSingleFilm(ctx, client, []int64{i})
	}
}

func searchBySingleVector(ctx context.Context, client milvus.MilvusClient, records []milvus.Entity) {
	defer measureTime(" search single record")()
	var topkQueryResult milvus.TopkQueryResult
	extraParams := "{\"nprobe\" : 32}"
	searchParam := milvus.SearchParam{collectionName, records, topk, nil, extraParams}
	topkQueryResult, status, err = client.Search(ctx, searchParam)
	if err != nil {
		println("Search rpc failed: " + err.Error())
	}
	println("total records", len(topkQueryResult.QueryResultList[0].Distances))
}

/*
   search milvus data by vector
*/
func searchByVector(ctx context.Context, client milvus.MilvusClient, wg *sync.WaitGroup) {
	defer wg.Done()
	defer measureTime("==============**********time taken to search 300 records ***************===============")()
	var i int64
	//Construct query vectors
	films, err := loadFilmCSV()
	if err != nil {
		log.Fatal("failed to load film data csv:", err.Error())
	}
	for i = 0; i < 300; i++ {
		vectors := make([][]float32, 0, 1)
		records := make([]milvus.Entity, 1)
		vectors = append(vectors, films[i].Vector[:]) // prevent same vector
		println(films[i].Vector[:])
		records[0].FloatData = vectors[0]
		searchBySingleVector(ctx, client, records)
	}
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
