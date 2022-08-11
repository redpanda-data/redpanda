package main

import (
	"flag"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"os"
	"reflect"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

var crc32c = crc32.MakeTable(crc32.Castagnoli)

func makeRandomBool() bool {
	return rand.Intn(2) == 1
}

func makeRandomSign(n int64) int64 {
	if makeRandomBool() {
		return -n
	}
	return n
}

func makeRandomInt64n(bounds int64) int64 {
	return makeRandomSign(rand.Int63n(bounds))
}

func makeRandomInt64() int64 {
	return makeRandomInt64n(math.MaxInt64)
}

func makeRandomString() string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, rand.Intn(50))
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func makeRandomTags() kmsg.Tags {
	var tags kmsg.Tags
	n := uint32(rand.Intn(10))
	for i := uint32(0); i < n; i++ {
		/// Make random bytes
		token := make([]byte, rand.Intn(50))
		rand.Read(token)
		/// Choosing 120 as an arbitrary number so the tag IDs of unknown tags
		/// never clash with those of known tags. Upon performing an audit the
		/// largest tag id is < 10, it is also close to the boundary of where
		/// a varint will take an extra byte of space
		tags.Set(120+i, token)
	}
	return tags
}

func makeRandomLegacyBatchV0() []kmsg.MessageV0 {
	n := rand.Intn(10)
	records := make([]kmsg.MessageV0, n)
	for i := 0; i < n; i++ {
		legacyRecord := kmsg.NewMessageV0()
		randomFillType(&legacyRecord, int16(-1))
		legacyRecord.Magic = int8(0)
		legacyRecord.Attributes = int8(0)
		serialized := legacyRecord.AppendTo(nil)
		legacyRecord.CRC = int32(crc32.ChecksumIEEE(serialized[8+4+4:]))
		legacyRecord.MessageSize = int32(len(serialized[8+4:]))
		records[i] = legacyRecord
	}
	return records
}

func makeRandomLegacyBatchV1() []kmsg.MessageV1 {
	n := rand.Intn(10)
	records := make([]kmsg.MessageV1, n)
	for i := 0; i < n; i++ {
		legacyRecord := kmsg.NewMessageV1()
		randomFillType(&legacyRecord, int16(-1))
		legacyRecord.Magic = int8(1)
		legacyRecord.Attributes = int8(0)
		serialized := legacyRecord.AppendTo(nil)
		legacyRecord.CRC = int32(crc32.ChecksumIEEE(serialized[8+4+4:]))
		legacyRecord.MessageSize = int32(len(serialized[8+4:]))
		records[i] = legacyRecord
	}
	return records
}

func makeRandomRecords() []kmsg.Record {
	n := rand.Intn(10)
	records := make([]kmsg.Record, n)
	for i := 0; i < n; i++ {
		record := kmsg.NewRecord()
		randomFillType(&record, int16(-1))
		recordSize := len(record.AppendTo(nil))
		record.Length = int32(recordSize) - 4
		records[i] = record
	}
	return records
}

func makeRandomRecordBatches() kmsg.RecordBatch {
	recordBatch := kmsg.NewRecordBatch()
	randomFillType(&recordBatch, int16(-1))
	records := makeRandomRecords()
	var bytearr []byte
	for _, record := range records {
		bytearr = append(bytearr, record.AppendTo(nil)...)
	}
	recordBatch.Records = bytearr
	recordBatch.NumRecords = int32(len(records))
	recordBatch.Magic = int8(2)       // new format
	recordBatch.Attributes = int16(0) // uncompressed
	rawBatch := recordBatch.AppendTo(nil)
	recordBatch.Length = int32(len(rawBatch[8+4:]))
	// skip first offset (int64) and length
	recordBatch.CRC = int32(crc32.Checksum(rawBatch[8+4+4+1+4:], crc32c))
	return recordBatch
}

func makeRandomKafkaData(version int16) []byte {
	var bytearr []byte
	if version >= 3 {
		data := makeRandomRecordBatches()
		bytearr = data.AppendTo(nil)
	} else if version == 2 {
		data := makeRandomLegacyBatchV1()
		for _, record := range data {
			bytearr = append(bytearr, record.AppendTo(nil)...)
		}
	} else {
		data := makeRandomLegacyBatchV0()
		for _, record := range data {
			bytearr = append(bytearr, record.AppendTo(nil)...)
		}
	}
	return bytearr
}

func randomFill(v reflect.Value, version int16) {
	switch v.Kind() {
	case reflect.Ptr:
		/// This conditional represents setting an nullable field to nil
		/// happening 50% of the time
		if makeRandomBool() {
			v.Set(reflect.New(v.Type().Elem()))
			randomFill(v.Elem(), version)
		}
	case reflect.Struct:
		if v.Type().Name() == "Tags" {
			v.Set(reflect.ValueOf(makeRandomTags()))
			return
		}
		for i := 0; i < v.NumField(); i++ {
			randomFill(v.Field(i), version)
		}
		// Special cases, the following byte arrays must be populated with kafka
		// record & recordBatch formatted data
		if v.Type().Name() == "ProduceRequestTopicPartition" {
			recordBatchData := makeRandomKafkaData(version)
			v.FieldByName("Records").Set(reflect.ValueOf(recordBatchData))
		} else if v.Type().Name() == "FetchResponseTopicPartition" {
			recordBatchData := makeRandomKafkaData(version)
			v.FieldByName("RecordBatches").Set(reflect.ValueOf(recordBatchData))
		}
	case reflect.Array:
		num := v.Len()
		arr := reflect.New(reflect.ArrayOf(num, v.Type().Elem())).Elem()
		v.Set(arr)
		for i := 0; i < num; i++ {
			randomFill(v.Index(i), version)
		}
	case reflect.Slice:
		num := rand.Intn(10)
		v.Set(reflect.MakeSlice(v.Type(), num, num))
		for i := 0; i < num; i++ {
			randomFill(v.Index(i), version)
		}
	case reflect.Bool:
		v.SetBool(makeRandomBool())
	case reflect.Uint8:
		v.SetUint(uint64(makeRandomInt64()))
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		v.SetInt(makeRandomInt64())
	case reflect.String:
		v.SetString(makeRandomString())
	default:
		fmt.Printf("Invalid type: %s\n", v.Kind())
		os.Exit(1)
	}
}

func randomFillType(p interface{}, version int16) {
	randomFill(reflect.ValueOf(p).Elem(), version)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	api := flag.Int("api", 0, "API key of request to generate")
	version := flag.Int("version", 0, "API version of generated request")
	isRequest := flag.Bool("is-request", true, "Generate kafka request if true, response if false")
	outputAsBinary := flag.Bool("binary-output", true, "Print result as binary")
	var result []byte
	flag.Parse()
	if *isRequest {
		req := kmsg.RequestForKey(int16(*api))
		randomFillType(req, int16(*version))
		req.SetVersion(int16(*version))
		result = req.AppendTo(nil)
	} else {
		resp := kmsg.ResponseForKey(int16(*api))
		randomFillType(resp, int16(*version))
		resp.SetVersion(int16(*version))
		result = resp.AppendTo(nil)
	}
	if *outputAsBinary {
		os.Stdout.Write(result[:])
	} else {
		fmt.Printf("%02x", result)
	}
}
