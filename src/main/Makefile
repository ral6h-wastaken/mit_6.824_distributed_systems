mrcoord: buildplug
	go run mrworker.go wc.so

buildplug: removeplug
	go build --buildmode=plugin ../mrapps/wc.go

removeplug:
	if [ -f wc.so ]; then rm wc.so; fi
