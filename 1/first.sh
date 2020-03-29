#!/bin/bash
rm mr-*
go build -buildmode=plugin ../mrapps/wc.go
nohup go run mrmaster.go pg-*.txt &
go run mrworker.go wc.so
go run mrworker.go wc.so
go run mrworker.go wc.so
go run mrworker.go wc.so
go run mrworker.go wc.so
go run mrworker.go wc.so
go run mrworker.go wc.so
go run mrworker.go wc.so
