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
	jobsCount := len(jobs)
	inChannels := make([]chan interface{}, jobsCount)
	outChannels := make([]chan interface{}, jobsCount)
	for j := 0; j < jobsCount; j++ {
		outChannels[j] = make(chan interface{}, 10)
		inChannels[j] = make(chan interface{}, 10)
	}

	for _, fibNum := range inputData {
		inChannels[0] <- fibNum
	}
	close(inChannels[0])

	for i, job := range jobs {
		if i == 0 {
			job(inChannels[i], outChannels[i])
		} else {
			job(outChannels[i-1], outChannels[i])
		}
	}
	close(outChannels[jobsCount-1])

	fmt.Println("ExecutePipeline finish")
	elapsed := time.Since(start)
	log.Printf("Execution time %s", elapsed)
}

func SingleHash(in, out chan interface{}) {
	fmt.Println("SingleHash start")
	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for rawIn := range in {
		wg.Add(1)
		dataMd5Chan := make(chan string, 1)
		dataCrc32Chan := make(chan string, 1)
		dataCrc32Md5Chan := make(chan string, 1)
		data := fmt.Sprintf("%v", rawIn)
		go func(dataCrc32Chan chan string, data string) {
			dataCrc32Chan <- DataSignerCrc32(data)
			close(dataCrc32Chan)
		}(dataCrc32Chan, data)
		go func(dataCrc32Md5Chan chan string) {
			dataMd5 := <-dataMd5Chan
			dataCrc32Md5Chan <- DataSignerCrc32(dataMd5)
			close(dataCrc32Md5Chan)
		}(dataCrc32Md5Chan)
		go func(dataCrc32Chan chan string) {
			defer wg.Done()
			fmt.Println("SingleHash rawIn", data)
			mu.Lock()
			dataMd5Chan <- DataSignerMd5(data)
			close(dataMd5Chan)
			mu.Unlock()

			dataCrc32 := <-dataCrc32Chan
			dataCrc32Md5 := <-dataCrc32Md5Chan
			result := dataCrc32 + "~" + dataCrc32Md5
			out <- result
		}(dataCrc32Chan)
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
