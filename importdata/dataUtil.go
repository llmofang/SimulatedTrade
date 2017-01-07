package importdata

import (
	"bufio"
	//	"finance"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

func ReadLine(fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		handler(line)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

func handler(line string) {
	fmt.Println("xxx:", line)
}

func Read(path string) string {
	fi, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	fmt.Println(string(fd))
	return string(fd)
}

func DoMain() {
	//	err := Read("D:\\stock\\000001.csv")
	//	fmt.Println("", err)
	//	f := handle()
	err := ReadLine("D:\\stock\\000001.csv")
	//		finance.Market
	fmt.Println("", err)
}
