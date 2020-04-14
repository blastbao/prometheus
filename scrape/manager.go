// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scrape

import (
	"encoding"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"

	"github.com/blastbao/prometheus/config"
	"github.com/blastbao/prometheus/discovery/targetgroup"
	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/storage"
	"github.com/prometheus/client_golang/prometheus"
)

var targetMetadataCache = newMetadataMetricsCollector()

// MetadataMetricsCollector is a Custom Collector for the metadata cache metrics.
type MetadataMetricsCollector struct {
	CacheEntries *prometheus.Desc
	CacheBytes   *prometheus.Desc

	scrapeManager *Manager
}

func newMetadataMetricsCollector() *MetadataMetricsCollector {
	return &MetadataMetricsCollector{
		CacheEntries: prometheus.NewDesc(
			"prometheus_target_metadata_cache_entries",
			"Total number of metric metadata entries in the cache",
			[]string{"scrape_job"},
			nil,
		),
		CacheBytes: prometheus.NewDesc(
			"prometheus_target_metadata_cache_bytes",
			"The number of bytes that are currently used for storing metric metadata in the cache",
			[]string{"scrape_job"},
			nil,
		),
	}
}

func (mc *MetadataMetricsCollector) registerManager(m *Manager) {
	mc.scrapeManager = m
}

// Describe sends the metrics descriptions to the channel.
func (mc *MetadataMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mc.CacheEntries
	ch <- mc.CacheBytes
}

// Collect creates and sends the metrics for the metadata cache.
func (mc *MetadataMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	if mc.scrapeManager == nil {
		return
	}

	for tset, targets := range mc.scrapeManager.TargetsActive() {
		var size, length int
		for _, t := range targets {
			size += t.MetadataSize()
			length += t.MetadataLength()
		}

		ch <- prometheus.MustNewConstMetric(
			mc.CacheEntries,
			prometheus.GaugeValue,
			float64(length),
			tset,
		)

		ch <- prometheus.MustNewConstMetric(
			mc.CacheBytes,
			prometheus.GaugeValue,
			float64(size),
			tset,
		)
	}
}



// NewManager is the Manager constructor
//



func NewManager(logger log.Logger, app storage.Appendable) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	m := &Manager{
		append:        app,
		logger:        logger,
		scrapeConfigs: make(map[string]*config.ScrapeConfig),
		scrapePools:   make(map[string]*scrapePool),
		graceShut:     make(chan struct{}),
		triggerReload: make(chan struct{}, 1),
	}
	targetMetadataCache.registerManager(m)

	return m
}




// Manager 负责维护 scrape pools，并且管理着 scrape 组件的生命周期。
//
// Manager 主要有以下函数：
//
// 	func (m *Manager) Run(tsets <-chan map[string][]targetgroup.Group) error
//	func (m *Manager) Stop()
//	func (m *Manager) ApplyConfig(cfg *config.Config) error
//




// Manager maintains a set of scrape pools and manages start/stop cycles
// when receiving new target groups form the discovery manager.
type Manager struct {
	logger    log.Logger
	append    storage.Appendable
	graceShut chan struct{}

	jitterSeed    uint64     // Global jitterSeed seed is used to spread scrape workload across HA setup.
	mtxScrape     sync.Mutex // Guards the fields below.
	scrapeConfigs map[string]*config.ScrapeConfig
	scrapePools   map[string]*scrapePool
	targetSets    map[string][]*targetgroup.Group

	triggerReload chan struct{}
}

// Run receives and saves target set updates and triggers the scraping loops reloading.
// Reloading happens in the background so that it doesn't block receiving targets updates.
//
// 译:
//
// Run 接收并保存 target set 的最新配置，并触发 scraping loops 的重新加载（reloading）。
// reloading 是在后台进行的，这样便不会阻塞接收新的配置更新。
//
// 名词解释:
//	target set: 数据采集目标集合
//
//
// 说明:
//
// 	Run() 函数由 main.go 中启动的 scrape manager goroutine 来调用。
// 	Run() 函数需要传入 discover 组件中的 SyncCh channel ，Run() 会监听 SyncCh channel，
//  一旦 SyncCh channel 有 message ，就会触发 manager 的 reload 函数。
//
//  在 reload() 函数中，会遍历 message 的数据，根据 jobName(tsetName) 从 scrapePools 中找，
//  如果找不到，则新建一个 scrapePool，如果 jobName(tsetName) 在 scrapeConfig 里面找不到，
//  那么就会打印一下错误信息。每一个 job(tset) 会创建一个对应的 scrapePool 实例。
//
//  reload() 函数最后会调用 sp.Sync(tgroup) 来更新 scrapePool 的信息。
//  通过 sync() 函数，就可以得出哪些 target 仍然是 active 的， 哪些 target 已经失效了。
//

func (m *Manager) Run(tsets <-chan map[string][]*targetgroup.Group) error {

	// 监听 m.triggerReload 信号，执行后台 reload() 操作。
	go m.reloader()

	for {
		select {
		// 监听 target set updates 事件
		case ts := <-tsets:

			// 1. 更新 m.targetSets 成员变量
			m.updateTsets(ts)

			// 2. 触发 m.reload() 重新加载
			select {
			case m.triggerReload <- struct{}{}:
			default: // 若管道满则立即返回，不阻塞
			}

		// 关闭信号
		case <-m.graceShut:
			return nil
		}
	}
}


// 监听 m.triggerReload 信号，执行后台 reload() 操作。
func (m *Manager) reloader() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		// 关闭信号
		case <-m.graceShut:
			return
		// 定时器，用于确保 reloading 最小间隔为 5s
		case <-ticker.C:
			// 监听 reloading 信号，执行 reload() 操作
			select {
			case <-m.triggerReload:
				m.reload()
			case <-m.graceShut:
				return
			}
		}
	}
}


// 执行 reload() 操作
func (m *Manager) reload() {
	// 加锁
	m.mtxScrape.Lock()
	var wg sync.WaitGroup

	// 遍历 targetSets，确保每个 targetSet 存在对应的 scrapePool 。
	for setName, groups := range m.targetSets {

		// 1. 检查该 targetSet 是否存在 scrapePool ，不存在则创建
		if _, ok := m.scrapePools[setName]; !ok {
			// 1.1 取出该 targetSet 的抓取配置，若不存在，则打印错误并跳过
			scrapeConfig, ok := m.scrapeConfigs[setName]
			if !ok {
				level.Error(m.logger).Log("msg", "error reloading target set", "err", "invalid config id:"+setName)
				continue
			}
			// 1.2 创建该 targetSet 的 scrapePool
			sp, err := newScrapePool(scrapeConfig, m.append, m.jitterSeed, log.With(m.logger, "scrape_pool", setName))
			if err != nil {
				level.Error(m.logger).Log("msg", "error creating new scrape pool", "err", err, "scrape_pool", setName)
				continue
			}
			// 1.3 保存
			m.scrapePools[setName] = sp
		}


		// 2.
		//
		// m.scrapePools[setName] 中存储着 targetSet 的 scrapePool
		// groups 中储着 targetSet 的




		wg.Add(1)

		// Run the sync in parallel as these take a while and at high load can't catch up.
		//
		// 并行运行，提升性能。
		go func(sp *scrapePool, groups []*targetgroup.Group) {
			sp.Sync(groups)
			wg.Done()
		}(m.scrapePools[setName], groups)

	}

	// 释放锁
	m.mtxScrape.Unlock()

	// 阻塞，等待所有协程退出
	wg.Wait()
}

// setJitterSeed calculates a global jitterSeed per server relying on extra label set.
//
// setJitterSeed 根据额外的标签集计算每台服务器的全局 jitterSeed 。
func (m *Manager) setJitterSeed(labels labels.Labels) error {
	h := fnv.New64a()
	hostname, err := getFqdn()
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(h, "%s%s", hostname, labels.String()); err != nil {
		return err
	}
	m.jitterSeed = h.Sum64()
	return nil
}

// Stop cancels all running scrape pools and blocks until all have exited.
func (m *Manager) Stop() {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	for _, sp := range m.scrapePools {
		sp.stop()
	}
	close(m.graceShut)
}

func (m *Manager) updateTsets(tsets map[string][]*targetgroup.Group) {
	m.mtxScrape.Lock()
	m.targetSets = tsets
	m.mtxScrape.Unlock()
}

// ApplyConfig resets the manager's target providers and job configurations as defined by the new cfg.
//
// 译:
// 	ApplyConfig 按照新 cfg 重置 manager 的 job 配置。
//
// 名词解释:
// 	target providers: 指标提供者
// 	job configurations: 指标抓取作业配置
//
// 说明:
//
// 	ApplyConfig() 函数是 prometheus 启动时或者 reload 配置文件时用到的。
// 	ApplyConfig() 会关闭并删除掉 reload 前存在的，但是新的 reload 配置文件没有的 job 所对应的 scrapePool 实例。
//
func (m *Manager) ApplyConfig(cfg *config.Config) error {

	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	// 初始化 map 结构，用于保存 job 的配置
	c := make(map[string]*config.ScrapeConfig)
	for _, scfg := range cfg.ScrapeConfigs {
		c[scfg.JobName] = scfg
	}
	m.scrapeConfigs = c

	// [?] 设置所有时间序列和警告与外部通信时用的外部标签 external_labels
	if err := m.setJitterSeed(cfg.GlobalConfig.ExternalLabels); err != nil {
		return err
	}

	// Cleanup and reload pool if the configuration has changed.
	//
	// 如果配置已经更改，清理历史配置，重新加载到池子中
	var failed bool

	// 遍历当前已缓存的抓取 job
	for name, sp := range m.scrapePools {

		// 如果当前 job 不存在，则删除
		if cfg, ok := m.scrapeConfigs[name]; !ok {
			sp.stop()
			delete(m.scrapePools, name)

		// 如果配置变更，启动 reload 重新加载
		} else if !reflect.DeepEqual(sp.config, cfg) {
			err := sp.reload(cfg)
			if err != nil {
				level.Error(m.logger).Log("msg", "error reloading scrape pool", "err", err, "scrape_pool", name)
				failed = true
			}
		}
	}

	// 失败 return
	if failed {
		return errors.New("failed to apply the new configuration")
	}
	return nil
}

// TargetsAll returns active and dropped targets grouped by job_name.
func (m *Manager) TargetsAll() map[string][]*Target {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	targets := make(map[string][]*Target, len(m.scrapePools))
	for tset, sp := range m.scrapePools {
		targets[tset] = append(sp.ActiveTargets(), sp.DroppedTargets()...)
	}
	return targets
}

// TargetsActive returns the active targets currently being scraped.
func (m *Manager) TargetsActive() map[string][]*Target {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	var (
		wg  sync.WaitGroup
		mtx sync.Mutex
	)

	targets := make(map[string][]*Target, len(m.scrapePools))
	wg.Add(len(m.scrapePools))
	for tset, sp := range m.scrapePools {
		// Running in parallel limits the blocking time of scrapePool to scrape
		// interval when there's an update from SD.
		go func(tset string, sp *scrapePool) {
			mtx.Lock()
			targets[tset] = sp.ActiveTargets()
			mtx.Unlock()
			wg.Done()
		}(tset, sp)
	}
	wg.Wait()
	return targets
}

// TargetsDropped returns the dropped targets during relabelling.
func (m *Manager) TargetsDropped() map[string][]*Target {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	targets := make(map[string][]*Target, len(m.scrapePools))
	for tset, sp := range m.scrapePools {
		targets[tset] = sp.DroppedTargets()
	}
	return targets
}

// getFqdn returns a FQDN if it's possible, otherwise falls back to hostname.
func getFqdn() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	ips, err := net.LookupIP(hostname)
	if err != nil {
		// Return the system hostname if we can't look up the IP address.
		return hostname, nil
	}

	lookup := func(ipStr encoding.TextMarshaler) (string, error) {
		ip, err := ipStr.MarshalText()
		if err != nil {
			return "", err
		}
		hosts, err := net.LookupAddr(string(ip))
		if err != nil || len(hosts) == 0 {
			return "", err
		}
		return hosts[0], nil
	}

	for _, addr := range ips {
		if ip := addr.To4(); ip != nil {
			if fqdn, err := lookup(ip); err == nil {
				return fqdn, nil
			}

		}

		if ip := addr.To16(); ip != nil {
			if fqdn, err := lookup(ip); err == nil {
				return fqdn, nil
			}

		}
	}
	return hostname, nil
}
