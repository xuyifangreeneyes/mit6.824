go build -buildmode=plugin ../mrapps/wc.go
go run mrmaster.go pg-*.txt
go run mrworker.go wc.so