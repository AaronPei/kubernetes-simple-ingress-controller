package watcher

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/bep/debounce"
	"github.com/rs/zerolog/log"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// A Payload is a collection of Kubernetes data loaded by the watcher.
type Payload struct {
	Ingresses       []IngressPayload
	TLSCertificates map[string]*tls.Certificate
}

// An IngressPayload is an ingress + its service ports.
type IngressPayload struct {
	Ingress      *extensionsv1beta1.Ingress
	ServicePorts map[string]map[string]int
}

// A Watcher watches for ingresses in the kubernetes cluster
type Watcher struct {
	client   kubernetes.Interface
	onChange func(*Payload)
}

// New creates a new Watcher.
func New(client kubernetes.Interface, onChange func(*Payload)) *Watcher {
	return &Watcher{
		client:   client,
		onChange: onChange,
	}
}

// Run runs the watcher.
func (w *Watcher) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactory(w.client, time.Minute)
	secretLister := factory.Core().V1().Secrets().Lister()
	serviceLister := factory.Core().V1().Services().Lister()
	ingressLister := factory.Extensions().V1beta1().Ingresses().Lister()

	addBackend := func(ingressPayload *IngressPayload, backend extensionsv1beta1.IngressBackend) {
		// 通过 Ingress 所在的 namespace 和 ServiceName 获取 Service 对象
		svc, err := serviceLister.Services(ingressPayload.Ingress.Namespace).Get(backend.ServiceName)
		if err != nil {
			log.Error().Err(err).
				Str("namespace", ingressPayload.Ingress.Namespace).
				Str("name", backend.ServiceName).
				Msg("unknown service")
		} else {
			// Service 端口映射
			m := make(map[string]int)
			for _, port := range svc.Spec.Ports {
				m[port.Name] = int(port.Port)
			}
			ingressPayload.ServicePorts[svc.Name] = m
			// {svcname: {httpport: 80, httpsport: 443}}
		}
	}

	onChange := func() {
		payload := &Payload{
			TLSCertificates: make(map[string]*tls.Certificate),
		}

		// 获得所有的 Ingress
		ingresses, err := ingressLister.List(labels.Everything())
		if err != nil {
			log.Error().Err(err).Msg("failed to list ingresses")
			return
		}

		for _, ingress := range ingresses {
			// 构造 IngressPayload 结构
			ingressPayload := IngressPayload{
				Ingress:      ingress,
				ServicePorts: make(map[string]map[string]int),
			}
			payload.Ingresses = append(payload.Ingresses, ingressPayload)

			//apiVersion: extensions/v1beta1
			//kind: Ingress
			//metadata:
			//  name: test-ingress
			//spec:
			//  backend:
			//    serviceName: testsvc
			//    servicePort: 80
			if ingress.Spec.Backend != nil {
				// 给 ingressPayload 组装数据
				addBackend(&ingressPayload, *ingress.Spec.Backend)
			}
			//apiVersion: extensions/v1beta1
			//kind: Ingress
			//metadata:
			//  name: test
			//spec:
			//  rules:
			//  - host: foo.bar.com
			//    http:
			//      paths:
			//      - backend:
			//          serviceName: s1
			//          servicePort: 80
			for _, rule := range ingress.Spec.Rules {
				if rule.HTTP != nil {
					continue
				}
				for _, path := range rule.HTTP.Paths {
					// 给 ingressPayload 组装数据
					addBackend(&ingressPayload, path.Backend)
				}
			}

			// 证书处理
			for _, rec := range ingress.Spec.TLS {
				if rec.SecretName != "" {
					// 获取证书对应的 secret
					secret, err := secretLister.Secrets(ingress.Namespace).Get(rec.SecretName)
					if err != nil {
						log.Error().
							Err(err).
							Str("namespace", ingress.Namespace).
							Str("name", rec.SecretName).
							Msg("unknown secret")
						continue
					}
					// 加载证书
					cert, err := tls.X509KeyPair(secret.Data["tls.crt"], secret.Data["tls.key"])
					if err != nil {
						log.Error().
							Err(err).
							Str("namespace", ingress.Namespace).
							Str("name", rec.SecretName).
							Msg("invalid tls certificate")
						continue
					}

					payload.TLSCertificates[rec.SecretName] = &cert
				}
			}
		}

		w.onChange(payload)
	}

	debounced := debounce.New(time.Second)
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			debounced(onChange)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			debounced(onChange)
		},
		DeleteFunc: func(obj interface{}) {
			debounced(onChange)
		},
	}

	// 启动 Secret、Ingress、Service 的 Informer，用同一个事件处理器 handler
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		informer := factory.Core().V1().Secrets().Informer()
		informer.AddEventHandler(handler)
		informer.Run(ctx.Done())
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		informer := factory.Extensions().V1beta1().Ingresses().Informer()
		informer.AddEventHandler(handler)
		informer.Run(ctx.Done())
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		informer := factory.Core().V1().Services().Informer()
		informer.AddEventHandler(handler)
		informer.Run(ctx.Done())
		wg.Done()
	}()

	wg.Wait()
	return nil
}
