package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/avro"
	"github.com/birdayz/kaf/pkg/proto"
	"github.com/golang/protobuf/jsonpb"
	"github.com/hokaccha/go-prettyjson"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
)

var (
	offsetNum   int
	latest      bool
	beginning   bool
	raw         bool
	follow      bool
	schemaCache *avro.SchemaCache
	keyfmt      *prettyjson.Formatter

	protoType string

	reg *proto.DescriptorRegistry
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().BoolVar(&latest, "latest", false, "latest offset")
	consumeCmd.Flags().BoolVar(&beginning, "beginning", true, "beginning offset")
	consumeCmd.Flags().IntVar(&offsetNum, "offset", 0, "Offset to start consuming, can use specific number, or '-5','-10'... to indicate latest-5, latest-10")

	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON")
	consumeCmd.Flags().BoolVarP(&follow, "follow", "f", false, "Shorthand to start consuming with offset HEAD-1 on each partition. Overrides --offset flag")
	consumeCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	consumeCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	consumeCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")

	keyfmt = prettyjson.NewFormatter()
	keyfmt.Newline = " " // Replace newline with space to avoid condensed output.
	keyfmt.Indent = 0

}

func getAvailableOffsetsRetry(
	ldr *sarama.Broker, req *sarama.OffsetRequest, d time.Duration,
) (*sarama.OffsetResponse, error) {
	var (
		err     error
		offsets *sarama.OffsetResponse
	)

	for {
		select {
		case <-time.After(d):
			return nil, err
		default:
			offsets, err = ldr.GetAvailableOffsets(req)
			if err == nil {
				return offsets, err
			}
		}
	}
}

func calculateOffset(topic string, partitions []int32) int64 {
	offs := make([]int64, 0)
	client := getClient()
	wg := sync.WaitGroup{}

	for partition := range partitions {

		wg.Add(1)

		// calling too fast
		time.Sleep(100 * time.Millisecond)

		go func(partition int32) {

			req := &sarama.OffsetRequest{
				Version: int16(1),
			}
			req.AddBlock(topic, partition, int64(-1), int32(0))
			broker, err := client.Leader(topic, partition)
			if err != nil {
				errorExit("Unable to get leader: %v\n", err)
			}

			offsets, err := broker.GetAvailableOffsets(req)
			if err != nil {
				errorExit("Unable to get available offsets: %v\n", err)
			}

			partitionOffset := offsets.GetBlock(topic, partition).Offset

			offs = append(offs, partitionOffset)

			wg.Done()
		}(int32(partition))
	}

	wg.Wait()

	// sort it
	sort.Slice(offs, func(i, j int) bool {
		return offs[i] > offs[j]
	})

	calculated := offs[0] + int64(offsetNum)
	if calculated < 0 {
		return 0
	} else {
		return calculated
	}
}

const (
	offsetsRetry       = 500 * time.Millisecond
	configProtobufType = "protobuf.type"
)

var consumeCmd = &cobra.Command{
	Use:    "consume",
	Short:  "Consume messages",
	Args:   cobra.ExactArgs(1),
	PreRun: setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		var offset int64

		client := getClient()

		consumer, err := sarama.NewConsumerFromClient(client)
		if err != nil {
			errorExit("Unable to create consumer from client: %v\n", err)
		}

		topic := args[0]

		partitions, err := consumer.Partitions(topic)
		if err != nil {
			errorExit("Unable to get partitions: %v\n", err)
		}

		if offsetNum > 0 {
			offset = int64(offsetNum)
		} else if offsetNum < 0 {
			offset = calculateOffset(topic, partitions)
		} else if latest {
			offset = sarama.OffsetNewest
		} else if beginning {
			offset = sarama.OffsetOldest
		} else {
			offset = sarama.OffsetOldest
		}

		schemaCache = getSchemaCache()

		wg := sync.WaitGroup{}
		mu := sync.Mutex{} // Synchronizes stderr and stdout.
		for _, partition := range partitions {

			wg.Add(1)

			go func(partition int32) {
				req := &sarama.OffsetRequest{
					Version: int16(1),
				}
				req.AddBlock(topic, partition, int64(-1), int32(0))
				ldr, err := client.Leader(topic, partition)
				if err != nil {
					errorExit("Unable to get leader: %v\n", err)
				}

				offsets, err := getAvailableOffsetsRetry(ldr, req, offsetsRetry)
				if err != nil {
					errorExit("Unable to get available offsets: %v\n", err)
				}
				partitionOffset := offsets.GetBlock(topic, partition).Offset
				if partitionOffset < offset {
					wg.Done()
					return
				}

				followOffset := offsets.GetBlock(topic, partition).Offset - 1

				if follow && followOffset > 0 {
					offset = followOffset
					fmt.Fprintf(os.Stderr, "Starting on partition %v with offset %v\n", partition, offset)
				}

				pc, err := consumer.ConsumePartition(topic, partition, offset)
				if err != nil {
					errorExit("Unable to consume partition: %v\n", err)
				}

				for msg := range pc.Messages() {
					var stderr bytes.Buffer

					// TODO make this nicer
					var dataToDisplay []byte
					if protoType != "" {
						dataToDisplay, err = protoDecode(reg, msg.Value, protoType)
						if err != nil {
							fmt.Fprintf(&stderr, "failed to decode proto. falling back to binary outputla. Error: %v", err)
						}
					} else {
						dataToDisplay, err = avroDecode(msg.Value)
						if err != nil {
							fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
						}
					}

					if !raw {
						formatted, err := prettyjson.Format(dataToDisplay)
						if err == nil {
							dataToDisplay = formatted
						}

						w := tabwriter.NewWriter(&stderr, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

						if len(msg.Headers) > 0 {
							fmt.Fprintf(w, "Headers:\n")
						}

						for _, hdr := range msg.Headers {
							var hdrValue string
							// Try to detect azure eventhub-specific encoding
							if len(hdr.Value) > 0 {
								switch hdr.Value[0] {
								case 161:
									hdrValue = string(hdr.Value[2 : 2+hdr.Value[1]])
								case 131:
									hdrValue = strconv.FormatUint(binary.BigEndian.Uint64(hdr.Value[1:9]), 10)
								default:
									hdrValue = string(hdr.Value)
								}
							}

							fmt.Fprintf(w, "\tKey: %v\tValue: %v\n", string(hdr.Key), hdrValue)

						}

						if msg.Key != nil && len(msg.Key) > 0 {

							key, err := avroDecode(msg.Key)
							if err != nil {
								fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
							}
							fmt.Fprintf(w, "Key:\t%v\n", formatKey(key))
						}
						fmt.Fprintf(w, "Partition:\t%v\nOffset:\t%v\nTimestamp:\t%v\n", msg.Partition, msg.Offset, msg.Timestamp)
						w.Flush()
					}

					mu.Lock()
					stderr.WriteTo(os.Stderr)
					colorable.NewColorableStdout().Write(dataToDisplay)
					fmt.Print("\n")
					mu.Unlock()
				}
				wg.Done()
			}(partition)
		}
		wg.Wait()

	},
}

// proto to JSON
func protoDecode(reg *proto.DescriptorRegistry, b []byte, _type string) ([]byte, error) {
	dynamicMessage := reg.MessageForType(_type)
	if dynamicMessage == nil {
		return b, nil
	}

	dynamicMessage.Unmarshal(b)

	var m jsonpb.Marshaler
	var w bytes.Buffer

	err := m.Marshal(&w, dynamicMessage)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil

}

func avroDecode(b []byte) ([]byte, error) {
	if schemaCache != nil {
		return schemaCache.DecodeMessage(b)
	}
	return b, nil
}

func formatKey(key []byte) string {
	b, err := keyfmt.Format(key)
	if err != nil {
		return string(key)
	}
	return string(b)
}
