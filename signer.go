package main

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"
)

func main() {
	j1 := SingleHash
	j2 := MultiHash
	j3 := CombineResults
	Jobs := []job{job(j1), job(j2), job(j3)}
	ExecutePipeline(Jobs...)
	fmt.Println("finish")
	fmt.Scanln()
}
func ExecutePipeline(jobs ...job) {
	start := time.Now()
	in1 := make(chan interface{}, 10)
	out1 := make(chan interface{}, 10)
	// in2 := make(chan interface{}, 10)
	out2 := make(chan interface{}, 100)
	// in3 := make(chan interface{}, 10)
	out3 := make(chan interface{}, 10)

	// inputData := []int{11, 22, 33, 44}
	inputData := []int{0, 1, 1, 2, 3, 5, 8}

	for _, fibNum := range inputData {
		in1 <- fibNum
	}
	close(in1)

	for i, job := range jobs {
		switch i {
		case 0:
			job(in1, out1)
		case 1:
			job(out1, out2)
		case 2:
			job(out2, out3)
		}
	}

	fmt.Println("ExecutePipeline finish")
	elapsed := time.Since(start)
	log.Printf("Execution time %s", elapsed)
}

func SingleHash(in, out chan interface{}) {
	fmt.Println("job0 start")

	var results []string
	for rawIn := range in {
		data := fmt.Sprintf("%v", rawIn)
		fmt.Println("job0 rawIn", data)

		result := DataSignerCrc32(data) + "~" + DataSignerCrc32(DataSignerMd5(data))
		results = append(results, result)
	}

	for _, element := range results {
		out <- element
	}

	close(out)
}

func MultiHash(in, out chan interface{}) {
	fmt.Println("job1  start")

	var results []string

	th := []int{0, 1, 2, 3, 4, 5}
	for rawIn := range in {
		data := fmt.Sprintf("%v", rawIn)
		resultString := ""
		for _, thItem := range th {
			thItem := strconv.Itoa(thItem)
			subResult := DataSignerCrc32(thItem + data)
			fmt.Println(data + " MultiHash: crc32(th+step1)) " + thItem + " " + subResult)
			resultString += subResult
		}

		results = append(results, resultString)
	}

	for _, element := range results {
		out <- element
	}
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
