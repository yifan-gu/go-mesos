package main

import (
	"net/http"
)

func main() {
	http.HandleFunc("/blackhole/mesos.internal.SmallMessage", func(http.ResponseWriter, *http.Request) {})
	http.HandleFunc("/blackhole/mesos.internal.MediumMessage", func(http.ResponseWriter, *http.Request) {})
	http.HandleFunc("/blackhole/mesos.internal.BigMessage", func(http.ResponseWriter, *http.Request) {})
	http.HandleFunc("/blackhole/mesos.internal.LargeMessage", func(http.ResponseWriter, *http.Request) {})
	http.ListenAndServe("localhost:8080", nil)
}
