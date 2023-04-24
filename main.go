package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"MemcloadGolang/appsinstalled"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

const (
	NormalErrorRate  = 0.01
	MemcMaxAttempts  = 3
	MemcAttemptDelay = 200 * time.Millisecond
)

var pattern string
var memcNewClient *memcache.Client

func createNewClient(addr string) *memcache.Client {
	memcNewClient = memcache.New(addr)
	memcNewClient.MaxIdleConns = 3
	memcNewClient.Timeout = 100 * time.Millisecond
	return memcNewClient
}

// AppsInstalled структура данных из одной строки логфайла
type AppsInstalled struct {
	devType string
	devId   string
	lat     float64
	lon     float64
	apps    []uint32
}

// Queue структура данных для обработки с помощью goroutines
type Queue struct {
	Clients map[string]*memcache.Client
	File    string
	Index   int
}

// Метод InsertAppsInstalled вставляет запись в Memcache по типу девайса
func (appsInstalled *AppsInstalled) InsertAppsInstalled(clients map[string]*memcache.Client) bool {
	ua := &appsinstalled.UserApps{
		Lon:  &appsInstalled.lat,
		Lat:  &appsInstalled.lon,
		Apps: appsInstalled.apps,
	}

	key := fmt.Sprintf("%s:%s", appsInstalled.devType, appsInstalled.devId)
	message, err := proto.Marshal(ua)
	if err != nil {
		log.Warnln("Could not serialize record:", appsInstalled)
		return false
	}

	client, ok := clients[appsInstalled.devType]
	if !ok {
		log.Warnln("Unexpected device type:", appsInstalled.devType)
		return false
	}

	item := memcache.Item{Key: key, Value: message}
	for attempt := 0; attempt < MemcMaxAttempts; attempt++ {
		err := client.Set(&item)
		if err != nil {
			time.Sleep(MemcAttemptDelay)
			continue
		}
		return true
	}
	log.Warnf("Cannot write to memc: %s\n", appsInstalled.devType)
	return false
}

// Функция ParseAppsinstalled разбирает строку и возвращает данные в виде объекта AppsInstalled
func ParseAppsinstalled(line string) (AppsInstalled, error) {
	var appsInstalled AppsInstalled

	parts := strings.Split(line, "\t")
	if len(parts) != 5 {
		return appsInstalled, errors.New("invalid format line")
	}

	appsInstalled.devType = parts[0]
	appsInstalled.devId = parts[1]

	lat, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return appsInstalled, errors.New("invalid geo coord `Lat`")
	}
	appsInstalled.lat = lat

	lon, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return appsInstalled, errors.New("invalid geo coord `Lon`")
	}
	appsInstalled.lon = lon

	appsList := strings.Split(parts[4], ",")
	for _, a := range appsList {
		app, err := strconv.ParseUint(a, 10, 32)
		if err != nil {
			continue
		}
		appsInstalled.apps = append(appsInstalled.apps, uint32(app))
	}

	return appsInstalled, nil
}

// Функция ProcessFile считывает и построчно обрабатывает данные из файла
func ProcessFile(clients map[string]*memcache.Client, path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	data, err := gzip.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	defer data.Close()

	_, fname := filepath.Split(path)
	log.Infof("Start processing file %s\n", fname)

	scanner := bufio.NewScanner(data)
	scanner.Split(bufio.ScanLines)

	var processed, failed float64 = 0.0, 0.0

	for scanner.Scan() {
		line := scanner.Text()
		processed++
		parseResult, err := ParseAppsinstalled(line)
		if err != nil {
			log.Warnf("%s for: %s", err, line)
			failed++
			continue
		}
		ok := parseResult.InsertAppsInstalled(clients)
		if !ok {
			failed++
		}
	}
	errorRate := failed / processed
	if processed > 0 && errorRate > NormalErrorRate {
		log.Errorf(
			"High error rate (%d > %d). Failed load %s \n",
			errorRate,
			NormalErrorRate,
			path,
		)
		return
	}
	log.Infof("Done %s, processed: %d , errors: %d", fname, int(processed), int(failed))
}

// Функция Worker
func Worker(queue chan Queue, results []chan string) {
	for q := range queue {
		ProcessFile(q.Clients, q.File)
		results[q.Index] <- q.File
	}
}

// Функция DotRename переименовывает обработанный логфайл, добавляя спереди к его имени "."
func DotRename(path string) {
	head, fn := filepath.Split(path)
	err := os.Rename(path, filepath.Join(head, "."+fn))
	if err != nil {
		log.Errorf("Error when renaming a file: %s", path)
	}
}

func main() {
	start := time.Now()
	flag.StringVar(&pattern, "pattern", "./data/appsinstalled/*.tsv.gz", "path to the log files")
	flag.Parse()

	clients := make(map[string]*memcache.Client)
	clients["idfa"] = createNewClient("127.0.0.1:33013")
	clients["gaid"] = createNewClient("127.0.0.1:33014")
	clients["adid"] = createNewClient("127.0.0.1:33015")
	clients["dvid"] = createNewClient("127.0.0.1:33016")

	files, err := filepath.Glob(pattern)

	if err != nil {
		log.Fatalf("No files found for the pattern %s", pattern)
	}

	n := 0
	for _, f := range files {
		_, fn := filepath.Split(f)
		if !strings.HasPrefix(fn, ".") {
			files[n] = f
			n++
		}
	}
	files = files[:n]
	numberFiles := len(files)
	sort.Strings(files)
	log.Infoln("Found", numberFiles, "files")

	tasks := make(chan Queue)
	results := make([]chan string, numberFiles)

	for i := 0; i < numberFiles; i++ {
		results[i] = make(chan string)
	}

	for i := 0; i < numberFiles; i++ {
		go Worker(tasks, results)
		tasks <- Queue{clients, files[i], i}
	}
	close(tasks)

	for i := 0; i < numberFiles; i++ {
		DotRename(<-results[i])
	}

	log.Printf("Execution time: %s", time.Since(start))
}
