package main

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

var httpHandler *HttpHandler

type HttpHandler struct {
	ch        chan *kafka.Message
	transport *http.Transport
	wg        sync.WaitGroup
}

func init() {
	httpHandler = &HttpHandler{
		transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          cfg.Forward.Concurrency,
			MaxIdleConnsPerHost:   cfg.Forward.Concurrency,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		ch: make(chan *kafka.Message, 1000),
	}
	httpHandler.Start()
}

func (h *HttpHandler) Start() {
	h.wg.Add(cfg.Forward.Concurrency)
	for i := 0; i < cfg.Forward.Concurrency; i++ {
		go func() {
			defer h.wg.Done()
			for msg := range h.ch {
				h.forward(msg)
			}
		}()
	}
	log.Info("HttpHandler Start")
}

func (h *HttpHandler) Stop() {
	close(h.ch)
	h.wg.Wait()
	log.Info("HttpHandler Stopped")
}

func (h *HttpHandler) Handle(msg *kafka.Message) {
	h.ch <- msg
}

func (h *HttpHandler) forward(msg *kafka.Message) {
	var taBody struct {
		AppId string `json:"appid"`
		// ignore other json fields
	}
	if err := json.Unmarshal(msg.Value, &taBody); err != nil {
		log.Errorf("json.Unmarshal error: %v, Value: %s", err, msg.Value)
		return
	}
	appk, ok := cfg.AppId[taBody.AppId]
	if !ok {
		return // ignore unknown appid
	}

	var body []byte
	if cfg.Forward.Compress {
		bs, err := h.compress(msg.Value)
		if err != nil {
			log.Errorf("compress error: %v", err)
			return
		}
		body = bs
	} else {
		body = msg.Value
	}

	br := bytes.NewReader(body)
	req, err := http.NewRequest(http.MethodPost, cfg.Forward.Url, br)
	if err != nil {
		log.Errorf("NewRequest error: %v, Value: %s", err, msg.Value)
		return
	}
	if cfg.Forward.Compress {
		req.Header.Set("Content-Encoding", "deflate")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Em-Appk", appk)

	for i := 0; i < cfg.Forward.MaxRetries; i++ {
		if i > 0 {
			_, err = br.Seek(0, io.SeekStart)
			if err != nil {
				log.Errorf("Seek error: %v", err)
				return
			}
		}
		res, err := h.transport.RoundTrip(req)
		if err != nil {
			if i == cfg.Forward.MaxRetries-1 {
				log.Warnf("Post error: %v, retries: %d", err, i+1)
				break
			}
			time.Sleep(time.Second * (1 << i))
			continue
		}
		resBody, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			if res.StatusCode >= 400 && res.StatusCode < 500 {
				if i == cfg.Forward.MaxRetries-1 {
					log.Warnf("Status error: %s, body: %s, retries: %d", res.Status, resBody, i+1)
					break
				}
				time.Sleep(time.Second * (1 << i))
				continue
			}
			log.Warnf("Status error: %s, body: %s", res.Status, resBody)
		}
		break
	}
}

func (h *HttpHandler) compress(bs []byte) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, int(float64(len(bs))*0.7)))
	w, err := flate.NewWriter(buf, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err = w.Write(bs); err != nil {
		return nil, err
	}
	if err = w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
