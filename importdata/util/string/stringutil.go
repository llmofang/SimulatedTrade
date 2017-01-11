package stringutil

import (
	"fmt"
	"strconv"
	//	"strings"
)

func StrToInt32(s string) int32 {

	x, err := strconv.Atoi(s)
	a := int32(x)
	if err != nil {
		fmt.Println(err)
	} else {
		return a
	}
	return a
}

func StrToFloat(s string) float64 {

	x, err := strconv.ParseFloat(s, 64)

	if err != nil {
		fmt.Println(err)
	} else {
		return x
	}
	return x
}
