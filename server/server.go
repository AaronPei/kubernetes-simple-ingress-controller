package server

import (
	"context"
	"crypto/tls"
	"fmt"
	stdlog "log"
	"net/http"
	"net/http/httputil"
	"sync/atomic"

	"github.com/cnych/kubernetes-simple-ingress-controller/watcher"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// A Server serves HTTP pages.
type Server struct {
	cfg          *config
	routingTable atomic.Value

	ready *Event
}

// New 创建一个新的服务器
func New(options ...Option) *Server {
	cfg := defaultConfig()
	for _, o := range options {
		o(cfg)
	}
	s := &Server{
		cfg:   cfg,
		ready: NewEvent(),
	}
	s.routingTable.Store(NewRoutingTable(nil))
	return s
}

// Run 启动服务器.
func (s *Server) Run(ctx context.Context) error {
	// 直到第一个 payload 数据后才开始监听
	s.ready.Wait(ctx)

	// 启动 80 和 443 两个端口
	var eg errgroup.Group
	eg.Go(func() error {
		// 当前的 Server 实现了 Handler 接口（ServeHTTP函数)
		srv := http.Server{
			Addr:    fmt.Sprintf("%s:%d", s.cfg.host, s.cfg.tlsPort),
			Handler: s,
		}
		srv.TLSConfig = &tls.Config{
			GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return s.routingTable.Load().(*RoutingTable).GetCertificate(hello.ServerName)
			},
		}
		log.Info().Str("addr", srv.Addr).Msg("starting secure HTTP server")
		err := srv.ListenAndServeTLS("", "")
		if err != nil {
			return fmt.Errorf("error serving tls: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		srv := http.Server{
			Addr:    fmt.Sprintf("%s:%d", s.cfg.host, s.cfg.port),
			Handler: s,
		}
		log.Info().Str("addr", srv.Addr).Msg("starting insecure HTTP server")
		err := srv.ListenAndServe()
		if err != nil {
			return fmt.Errorf("error serving non-tls: %w", err)
		}
		return nil
	})
	return eg.Wait()
}

// ServeHTTP serves an HTTP request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 获取后端的真实服务地址
	backendURL, err := s.routingTable.Load().(*RoutingTable).GetBackend(r.Host, r.URL.Path)
	if err != nil {
		http.Error(w, "upstream server not found", http.StatusNotFound)
		return
	}
	log.Info().Str("host", r.Host).Str("path", r.URL.Path).Str("backend", backendURL.String()).Msg("proxying request")
	// 使用 NewSingleHostReverseProxy 进行代理请求
	p := httputil.NewSingleHostReverseProxy(backendURL)
	p.ErrorLog = stdlog.New(log.Logger, "", 0)
	p.ServeHTTP(w, r)
}

// Update 更新路由表根据新的 Ingress 规则
func (s *Server) Update(payload *watcher.Payload) {
	s.routingTable.Store(NewRoutingTable(payload))
	s.ready.Set()
}
