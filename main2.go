package main

import (
	"github.com/llmofang/SimulatedTrade/finance"
	"fmt"

	"time"
)

func main() {
	fmt.Println(time.Now())
	finance.DoMain()
}
