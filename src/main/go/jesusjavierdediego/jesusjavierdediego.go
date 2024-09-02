package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"slices"
)

const (
	chunkSize = 128 * 1024 * 1024 // 1 MB chunk size
	filePath  = "measurements-hundred.txt"
	outputFilePath = "output.txt"
	numOfChunks      = 128 // Number of chunks to split the file into
	numberOfStations = 416
)

type Outcome struct {
	Station string
	Min     float32
	Max     float32
	Avg     float32
}


// func processLine(line string, localMap *sync.Map) {
//     parts := strings.Split(line, ";")
//     if len(parts) != 2 {
//         return
//     }
//     key := parts[0]
//     value, err := strconv.ParseFloat(parts[1], 32)
//     if err != nil {
//         fmt.Println("Error converting string to float32:", err)
//         return
//     }

//     actual, _ := localMap.LoadOrStore(key, &[]float32{})
//     recordSlice := actual.(*[]float32)
//     *recordSlice = append(*recordSlice, float32(value))
// }

func mergeSyncMaps(dest, src *sync.Map) {
    src.Range(func(key, value interface{}) bool {
        dest.Store(key, value)
        return true
    })
}

func calculateOutcome(station string, values []float32, ch chan Outcome, wg3 *sync.WaitGroup) {
	defer wg3.Done()
	res := fmt.Sprintf("%.2f", sum(values, len(values))/float32(len(values)))
	average, _ := strconv.ParseFloat(res, 32)
	ch <- Outcome{Station: station, Min: slices.Min(values), Max: slices.Max(values), Avg: float32(average)}
}

func writeListToFile(outputlist []Outcome) {
	file, err := os.Create(outputFilePath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	for _, outcome := range outputlist {
		_, err := file.WriteString(fmt.Sprintf("%s;%.2f;%.2f;%.2f\n", outcome.Station, outcome.Min, outcome.Max, outcome.Avg))
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}
}

func sum(arr []float32, n int) float32 {
	if n <= 0 {
		return 0
	}
	return sum(arr, n-1) + arr[n-1]
}

func main() {
	start := time.Now()

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	chunkChannel := make(chan string)
	var wg sync.WaitGroup

	// Get the file size
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return
	}
	fileSize := fileInfo.Size()

	// chunkSize := fileSize / int64(64)
	// fmt.Println("Chunk size:", chunkSize)

	// Read the file in chunks
	// sectionReader := io.NewSectionReader(file, 0, chunkSize)
	var buffer []byte
	offset := int64(0)

	// counter	:= 0
	for offset < fileSize {
		// counter = counter +1
		// fmt.Println("counter:", counter)
		sectionReader := io.NewSectionReader(file, offset, chunkSize)
		part := make([]byte, chunkSize)
		n, err := sectionReader.Read(part)
		if n > 0 {
			buffer = append(buffer, part[:n]...)
			// // Find the last newline character in the buffer
			lastNewline := -1

			for i := len(buffer) - 1; i >= 0; i-- {
				if buffer[i] == '\n' {
					lastNewline = i
					break
				}
			}
			if lastNewline != -1 {
				// Send the chunk up to the last newline
				wg.Add(1)
				go func(chunk []byte) {
					defer wg.Done()
					chunkChannel <- string(chunk)
				}(buffer[:lastNewline+1])
				// Keep the remaining part in the buffer
				buffer = buffer[lastNewline+1:]
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Error reading file:", err)
			return
		}

		offset += int64(n)
	}

	// Send any remaining buffer as the last chunk
	if len(buffer) > 0 {
		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			chunkChannel <- string(chunk)
		}(buffer)
	}

	// Close the chunk channel after all chunks are sent
	go func() {
		wg.Wait()
		close(chunkChannel)
	}()

	readElapsed := time.Since(start)
	fmt.Println("Time taken to split the file in chunks:", readElapsed)
	startRead := time.Now()

	mapOfRecords := sync.Map{}
	var wg2 sync.WaitGroup

	// Read the chunks from the channel
	for c := range chunkChannel {
		wg2.Add(1)
		go func(c string) {
			defer wg2.Done()
			AllLines := strings.Split(c, "\n")
			// fmt.Println("Length of all  lines", len(AllLines))
			splittedLines := splitArrayIntoParts(AllLines, 16)
			for _, chunkOfLines := range splittedLines {
				// fmt.Println("Length of chunk of lines", len(lines))
				var wg3 sync.WaitGroup
				wg3.Add(1)
				go func(chunkOfLines []string) {
					localMap := &sync.Map{}
					defer wg3.Done()
					for _, line := range chunkOfLines {
						parts := strings.Split(line, ";")
						if len(parts) != 2 {
							continue
						}
						value, err := strconv.ParseFloat(parts[1], 32)
						if err != nil {
							fmt.Println("Error converting string to float32:", err)
							continue
						}

						actual, _ := localMap.LoadOrStore(parts[0], &[]float32{})
						recordSlice := actual.(*[]float32)
						*recordSlice = append(*recordSlice, float32(value))
					}
					mergeSyncMaps(&mapOfRecords, localMap)
				}(chunkOfLines)
				wg3.Wait()
			}

		}(c)
	}
	wg2.Wait()

	fmt.Println("Time taken to read the lines in all the chunks:", time.Since(startRead))
	startCalc := time.Now()

	// First records in file
	if values, ok := mapOfRecords.Load("Huesca"); ok {
		fmt.Println("Number of values for Huesca:", len(*values.(*[]float32)))
		fmt.Println("Values for Huesca:", *values.(*[]float32))
	} else {
		fmt.Println("Huesca key not found")
	}

	// records in the middle of file
	if values, ok := mapOfRecords.Load("Zaragoza"); ok {
		fmt.Println("Number of values for Zaragoza:", len(*values.(*[]float32)))
		fmt.Println("Values for Zaragoza:", *values.(*[]float32))
	} else {
		fmt.Println("Zaragoza key not found")
	}

	// Last records in file
	if values, ok := mapOfRecords.Load("Teruel"); ok {
		fmt.Println("Number of values for Teruel:", len(*values.(*[]float32)))
		fmt.Println("Values for Teruel:", *values.(*[]float32))
	} else {
		fmt.Println("Teruel key not found")
	}

	var finalOutcomeList []Outcome
	var wg3 sync.WaitGroup
	var outcomeListChannel = make(chan Outcome, numberOfStations)
	mapOfRecords.Range(func(key, value interface{}) bool {
		wg3.Add(1)
		//fmt.Printf("MAP -- Key: %s, Values: %v\n", key, value)

		go calculateOutcome(key.(string), *value.(*[]float32), outcomeListChannel, &wg3)

		return true
	})

	go func() {
		wg3.Wait()
		close(outcomeListChannel)
	}()

	fmt.Println("Time taken to calculate all stations:", time.Since(startCalc))
	startList := time.Now()

	for s := range outcomeListChannel {
		finalOutcomeList = append(finalOutcomeList, s)
	}

	fmt.Println("Time taken get the final list:", time.Since(startList))
	startWrite := time.Now()

	writeListToFile(finalOutcomeList)

	fmt.Println("Time taken to write the values in the file:", time.Since(startWrite))

	fmt.Println("Time TOTAL:", time.Since(start))

}

func splitArrayIntoParts(lines []string, numParts int) [][]string {
    length := len(lines)
    partSize := (length + numParts - 1) / numParts // Calculate the size of each part, rounding up

    var result [][]string

    for i := 0; i < numParts; i++ {
        start := i * partSize
        end := start + partSize
        if end > length {
            end = length
        }
        result = append(result, lines[start:end])
    }

    return result
}