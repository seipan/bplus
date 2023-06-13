# btree
btree

This cli tool compares speeds in btree and map, respectively. This allows you to experience the speed of indexes used in databases and other applications.
## Install
```
go install github.com/seipan/btree/cmd/btree@latest
```

## Example
```
$ go run main.go --N=60000000
--------------------------- default map create ---------------------------
--------------------------- default map create ---------------------------
2023/06/14 04:28:17 37.1181537s
--------------------------- default map get ---------------------------
--------------------------- default map get ---------------------------
2023/06/14 04:28:18 514.2µs
--------------------------- btree create ---------------------------
--------------------------- btree create ---------------------------
2023/06/14 04:28:36 18.6992265s
--------------------------- btree get ---------------------------
--------------------------- btree get ---------------------------
2023/06/14 04:28:36 0s
```
## Prior art
https://github.com/google/btree
