package main

import (
	"context"
	"flag"
	"github.com/cnych/kubernetes-simple-ingress-controller/server"
	"github.com/cnych/kubernetes-simple-ingress-controller/watcher"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"os"
)

var (
	host          string
	port, tlsPort int
)

func main() {
	flag.StringVar(&host, "host", "0.0.0.0", "the host to bind")
	flag.IntVar(&port, "port", 80, "the insecure http port")
	flag.IntVar(&tlsPort, "tls-port", 443, "the secure https port")
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// ErrorHandlers 是一个函数列表，当发生一些错误时，会调用这些函数。
	runtime.ErrorHandlers = []func(error) {
		func(err error) {
			log.Warn().Err(err).Msg("[k8s]")
		},
	}

	// 从集群内的token和ca.crt获取 Config
	config, err := rest.InClusterConfig()
	// 由于我们要通过集群内部的 Service 进行服务的访问，所以不能在集群外部使用，所以不能使用 kubeconfig 的方式来获取 Config
	if err != nil {
		log.Fatal().Err(err).Msg("get kubernetes configuration failed")
	}

	// 从 Config 中创建一个新的 Clientset
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal().Err(err).Msg("create kubernetes client failed")
	}

	s := server.New(server.WithHost(host), server.WithPort(port), server.WithTLSPort(tlsPort))
	w := watcher.New(client, func(payload *watcher.Payload) {
		s.Update(payload)
	})

	var eg errgroup.Group
	eg.Go(func() error {
		return s.Run(context.TODO())
	})
	eg.Go(func() error {
		return w.Run(context.TODO())
	})
	if err := eg.Wait(); err != nil {
		log.Fatal().Err(err).Send()
	}
}
