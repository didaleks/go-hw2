package main

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
)

// inputData := []int{11, 22, 33, 44}
// var inputData = []int{0, 1, 2}
// var inputData = []int{0}
var inputData = []int{0, 1, 1, 2, 3, 5, 8}

func main() {
	j1 := SingleHash
	j2 := MultiHash
	j3 := CombineResults
	Jobs := []job{job(j1), job(j2), job(j3)}
	ExecutePipeline(Jobs...)
	fmt.Println("finish")
}
func ExecutePipeline(jobs ...job) {
	start := time.Now()
	in1 := make(chan interface{}, 10)
	out1 := make(chan interface{}, 10)
	// in2 := make(chan interface{}, 10)
	out2 := make(chan interface{}, 100)
	// in3 := make(chan interface{}, 10)
	out3 := make(chan interface{}, 10)

	for _, fibNum := range inputData {
		in1 <- fibNum
	}
	close(in1)

	for i, job := range jobs {
		switch i {
		case 0:
			job(in1, out1)
			elapsed := time.Since(start)
			log.Printf("Execution time %s", elapsed)
		case 1:
			job(out1, out2)
			elapsed := time.Since(start)
			log.Printf("Execution time %s", elapsed)
		case 2:
			job(out2, out3)
		}
	}
	close(out3)

	fmt.Println("ExecutePipeline finish")
	elapsed := time.Since(start)
	log.Printf("Execution time %s", elapsed)
}

func SingleHash(in, out chan interface{}) {
	fmt.Println("job0 start")
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	for rawIn := range in {
		wg.Add(1)
		data := fmt.Sprintf("%v", rawIn)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			fmt.Println("job0 rawIn", data)
			// todo parallel hash operations
			mu.Lock()
			dataMd5 := DataSignerMd5(data)
			mu.Unlock()
			result := DataSignerCrc32(data) + "~" + DataSignerCrc32(dataMd5)
			out <- result
		}(wg)
	}

	wg.Wait()
	close(out)
}

func MultiHash(in, out chan interface{}) {
	fmt.Println("job1  start")
	var calculateTh = func(data string, th []int, resultStrings chan interface{}, wgExternal *sync.WaitGroup) {
		defer wgExternal.Done()
		wgInternal := &sync.WaitGroup{}
		mu := &sync.Mutex{}
		subResultsMap := map[int]string{}
		resultString := ""
		for _, thItem := range th {

			wgInternal.Add(1)
			i := strconv.Itoa(thItem)
			thItem := thItem
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				subResult := DataSignerCrc32(i + data)
				fmt.Println(data + " MultiHash: crc32(th+step1)) " + i + " " + subResult)
				mu.Lock()
				subResultsMap[thItem] = subResult
				mu.Unlock()
			}(wgInternal)
		}
		wgInternal.Wait()
		// sort map
		keys := make([]int, 0, len(subResultsMap))
		for k := range subResultsMap {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, k := range keys {
			fmt.Println("subResultsMap[k]", k, subResultsMap[k])
			resultString += subResultsMap[k]
		}
		resultStrings <- resultString
	}

	th := []int{0, 1, 2, 3, 4, 5}
	wg := &sync.WaitGroup{}
	for rawIn := range in {
		data := fmt.Sprintf("%v", rawIn)
		wg.Add(1)
		go calculateTh(data, th, out, wg)
	}
	wg.Wait()
	close(out)
}

func CombineResults(in, out chan interface{}) {
	fmt.Println("CombineResults  start")
	var results []string
	resultString := ""

	for rawIn := range in {
		data := fmt.Sprintf("%v", rawIn)
		results = append(results, data)
	}
	sort.Strings(results)
	for i, result := range results {
		fmt.Println("CombineResults result range:", i, result)
		if i != 0 {
			resultString += "_"
		}
		resultString += result
	}

	fmt.Println("job2 result:", resultString)
	out <- resultString
	// close(out)
}
