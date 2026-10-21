// Baseline demonstrates placement change; it does not move any stored data.
package main

import (
	"fmt"
	"log"

	"generation-fenced-shard-transfer/internal/placement"
)

func main() {
	before, err := placement.NewBaseline([]string{"shard-A", "shard-B", "shard-C"}, 100)
	if err != nil {
		log.Fatal(err)
	}
	after, err := placement.NewBaseline([]string{"shard-A", "shard-B", "shard-C", "shard-D"}, 100)
	if err != nil {
		log.Fatal(err)
	}
	moved := 0
	const total = 10000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("entity-%d", i)
		old, next := before.Lookup(key), after.Lookup(key)
		if old != next {
			if moved < 5 {
				fmt.Printf("%s: %s -> %s\n", key, old, next)
			}
			moved++
		}
	}
	fmt.Printf("Moved placement: %d/%d keys (%.2f%%)\n", moved, total, 100*float64(moved)/total)
	fmt.Println("Placement only: no database state was copied and no ownership was transferred.")
}
