// Copyright 2016 The Prometheus Authors
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
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"

	"github.com/blastbao/prometheus/config"
	"github.com/blastbao/prometheus/discovery/targetgroup"
	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/pkg/pool"
	"github.com/blastbao/prometheus/pkg/relabel"
	"github.com/blastbao/prometheus/pkg/textparse"
	"github.com/blastbao/prometheus/pkg/timestamp"
	"github.com/blastbao/prometheus/pkg/value"
	"github.com/blastbao/prometheus/storage"
)

var errNameLabelMandatory = fmt.Errorf("missing metric name (%s label)", labels.MetricName)

var (
	targetIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_interval_length_seconds",
			Help:       "Actual intervals between scrapes.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"interval"},
	)
	targetReloadIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_reload_length_seconds",
			Help:       "Actual interval to reload the scrape pool with a given configuration.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"interval"},
	)
	targetScrapePools = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pools_total",
			Help: "Total number of scrape pool creation attempts.",
		},
	)
	targetScrapePoolsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pools_failed_total",
			Help: "Total number of scrape pool creations that failed.",
		},
	)
	targetScrapePoolReloads = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_reloads_total",
			Help: "Total number of scrape loop reloads.",
		},
	)
	targetScrapePoolReloadsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_reloads_failed_total",
			Help: "Total number of failed scrape loop reloads.",
		},
	)
	targetSyncIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_sync_length_seconds",
			Help:       "Actual interval to sync the scrape pool.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"scrape_job"},
	)
	targetScrapePoolSyncsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_sync_total",
			Help: "Total number of syncs that were executed on a scrape pool.",
		},
		[]string{"scrape_job"},
	)
	targetScrapeSampleLimit = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_exceeded_sample_limit_total",
			Help: "Total number of scrapes that hit the sample limit and were rejected.",
		},
	)
	targetScrapeSampleDuplicate = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_duplicate_timestamp_total",
			Help: "Total number of samples rejected due to duplicate timestamps but different values",
		},
	)
	targetScrapeSampleOutOfOrder = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_out_of_order_total",
			Help: "Total number of samples rejected due to not being out of the expected order",
		},
	)
	targetScrapeSampleOutOfBounds = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_out_of_bounds_total",
			Help: "Total number of samples rejected due to timestamp falling outside of the time bounds",
		},
	)
	targetScrapeCacheFlushForced = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_cache_flush_forced_total",
			Help: "How many times a scrape cache was flushed due to getting big while scrapes are failing.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		targetIntervalLength,
		targetReloadIntervalLength,
		targetScrapePools,
		targetScrapePoolsFailed,
		targetScrapePoolReloads,
		targetScrapePoolReloadsFailed,
		targetSyncIntervalLength,
		targetScrapePoolSyncsCounter,
		targetScrapeSampleLimit,
		targetScrapeSampleDuplicate,
		targetScrapeSampleOutOfOrder,
		targetScrapeSampleOutOfBounds,
		targetScrapeCacheFlushForced,
		targetMetadataCache,
	)
}

// scrapePool manages scrapes for sets of targets.
//
// [!]
// 在 prometheus 中，一个 job 对应一个 scrapePool 实例。
// 我们知道，一个 job 可能存在多个 target ，在 job 关联的 scrapePool 实例中，
// 为每个 target 创建一个 scrapeLoop 对象， 该对象负责该 target 上指标数据的采集、转换和存储。
//
//
// scrapePool 有以下函数：
// 	func (sp *scrapePool) stop()
//	func (sp *scrapePool) reload(cfg *config.ScrapeConfig)
//	func (sp scrapePool) Sync(tgs []targetgroup.Group) (tActive []Target, tDropped []Target)
//	func (sp scrapePool) sync(targets []Target)
//
// 其中最重要的函数是 sync() 函数。
//
// sync() 会根据入参 targets 列表与原有的 targets 列表比对，
// 	如果有新添加的 targets，会创建新的 targetScraper 和 loop，并且启动新的 loop 。
//  如果有已失效的 targets，会  stop 这部分 targets 并从列表中删除。
//
// 如何理解 loop 呢？
// 	prometheus 是拉模型，需要定时到监控目标上拉取相应的数据，loop 就是管理何时进行拉取操作的。
// 	每个 loop 都是用一个 goroutine 来 run 的，在 loop 内可以控制何时进行 scraper 操作。
//
//
//
//
type scrapePool struct {
	appendable storage.Appendable
	logger     log.Logger

	// 读写锁
	mtx sync.RWMutex

	// 抓取配置
	config *config.ScrapeConfig

	// http client
	client *http.Client

	// Targets and loops must always be synchronized to have the same set of hashes.

	// 正在运行的 targets
	activeTargets map[uint64]*Target

	// 无效的 targets
	droppedTargets []*Target

	// 所有运行的 loop
	loops map[uint64]loop

	// 取消
	cancel context.CancelFunc

	// 创建 scrape loop
	//
	// Constructor for new scrape loops.
	// This is settable for testing convenience.
	newLoop func(scrapeLoopOptions) loop
}

type scrapeLoopOptions struct {
	target          *Target
	scraper         scraper
	limit           int
	honorLabels     bool
	honorTimestamps bool
	mrc             []*relabel.Config
	cache           *scrapeCache
}

const maxAheadTime = 10 * time.Minute

type labelsMutator func(labels.Labels) labels.Labels

func newScrapePool(cfg *config.ScrapeConfig, app storage.Appendable, jitterSeed uint64, logger log.Logger) (*scrapePool, error) {

	// increase counter metric
	targetScrapePools.Inc()
	if logger == nil {
		logger = log.NewNopLogger()
	}

	// 创建 http client ，用于执行数据抓取
	client, err := config_util.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName, false)
	if err != nil {
		targetScrapePoolsFailed.Inc()
		return nil, errors.Wrap(err, "error creating HTTP client")
	}

	// 设置 buffers
	buffers := pool.New(1e3, 100e6, 3, func(sz int) interface{} { return make([]byte, 0, sz) })

	// 设置 scrapePool 的一些基础属性
	ctx, cancel := context.WithCancel(context.Background())
	sp := &scrapePool{
		cancel:        cancel,
		appendable:    app,
		config:        cfg,
		client:        client,
		activeTargets: map[uint64]*Target{},
		loops:         map[uint64]loop{},
		logger:        logger,
	}

	// newLoop 用于为 target 创建 loop 。
	sp.newLoop = func(opts scrapeLoopOptions) loop {

		// Update the targets retrieval function for metadata to a new scrape cache.
		cache := opts.cache
		if cache == nil {
			cache = newScrapeCache()
		}
		opts.target.SetMetadataStore(cache)

		return newScrapeLoop(
			ctx,
			opts.scraper,
			log.With(logger, "target", opts.target),
			buffers,
			func(l labels.Labels) labels.Labels {
				return mutateSampleLabels(l, opts.target, opts.honorLabels, opts.mrc)
			},
			func(l labels.Labels) labels.Labels { return mutateReportSampleLabels(l, opts.target) },
			func() storage.Appender { return appender(app.Appender(), opts.limit) },
			cache,
			jitterSeed,
			opts.honorTimestamps,
		)
	}

	return sp, nil
}

func (sp *scrapePool) ActiveTargets() []*Target {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	var tActive []*Target
	for _, t := range sp.activeTargets {
		tActive = append(tActive, t)
	}
	return tActive
}

func (sp *scrapePool) DroppedTargets() []*Target {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()
	return sp.droppedTargets
}

// stop terminates all scrape loops and returns after they all terminated.
func (sp *scrapePool) stop() {
	sp.cancel()
	var wg sync.WaitGroup

	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	for fp, l := range sp.loops {
		wg.Add(1)

		go func(l loop) {
			l.stop()
			wg.Done()
		}(l)

		delete(sp.loops, fp)
		delete(sp.activeTargets, fp)
	}
	wg.Wait()
	sp.client.CloseIdleConnections()
}

// reload the scrape pool with the given scrape configuration.
//
// The target state is preserved but all scrape loops are restarted with the new scrape configuration.
// This method returns after all scrape loops that were stopped have stopped scraping.
func (sp *scrapePool) reload(cfg *config.ScrapeConfig) error {
	targetScrapePoolReloads.Inc()
	start := time.Now()

	// 加解锁
	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	// 生成 http client ，用于获取指标(metircs)
	client, err := config_util.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName, false)
	if err != nil {
		targetScrapePoolReloadsFailed.Inc()
		return errors.Wrap(err, "error creating HTTP client")
	}

	reuseCache := reusableCache(sp.config, cfg)
	sp.config = cfg
	oldClient := sp.client
	sp.client = client

	var (
		wg              sync.WaitGroup
		interval        = time.Duration(sp.config.ScrapeInterval)
		timeout         = time.Duration(sp.config.ScrapeTimeout)
		limit           = int(sp.config.SampleLimit)
		honorLabels     = sp.config.HonorLabels
		honorTimestamps = sp.config.HonorTimestamps
		mrc             = sp.config.MetricRelabelConfigs
	)

	// 停止该 scrapePool 下对应的所有的 oldLoop ，根据配置创建所有的 newLoop ，并通过协程启动。
	for fp, oldLoop := range sp.loops {
		var cache *scrapeCache
		if oc := oldLoop.getCache(); reuseCache && oc != nil {
			oldLoop.disableEndOfRunStalenessMarkers()
			cache = oc
		} else {
			cache = newScrapeCache()
		}
		var (
			t       = sp.activeTargets[fp]
			s       = &targetScraper{Target: t, client: sp.client, timeout: timeout}
			newLoop = sp.newLoop(scrapeLoopOptions{
				target:          t,
				scraper:         s,
				limit:           limit,
				honorLabels:     honorLabels,
				honorTimestamps: honorTimestamps,
				mrc:             mrc,
				cache:           cache,
			})
		)
		wg.Add(1)

		go func(oldLoop, newLoop loop) {
			oldLoop.stop()
			wg.Done()

			go newLoop.run(interval, timeout, nil)
		}(oldLoop, newLoop)

		sp.loops[fp] = newLoop
	}

	wg.Wait()
	oldClient.CloseIdleConnections()
	targetReloadIntervalLength.WithLabelValues(interval.String()).Observe(
		time.Since(start).Seconds(),
	)
	return nil
}

// Sync converts target groups into actual scrape targets and synchronizes
// the currently running scraper with the resulting set and returns all scraped and dropped targets.
//
// Sync 函数是一个对外暴露函数的接口：
//
//	1. 把从配置解析出来的 target 结构化
//  2. 调用内部方法 sync() 来进行数据抓取的执行
//  3. 一些计数器添加计数
//
// 值得注意的是 Append 方法，是一个封装了的方法，是同时进行对变量的修改，并且包含了采集到的数据持久化的操作。
//
//
// sp.Sync() 方法把 []*targetgroup.Group 类型的 groups 转换为 []*Target 类型，
// 其中每个 groups 对应一个 job_name 下多个 targets。
// 随后，调用 sp.sync() 方法，同步 scrape 服务。


func (sp *scrapePool) Sync(tgs []*targetgroup.Group) {
	start := time.Now()

	var all []*Target

	// 加锁
	sp.mtx.Lock()

	sp.droppedTargets = []*Target{}

	// 遍历所有 Target Group
	for _, tg := range tgs {

		// 将 tg 从 targetgroup.Group 转换为 []*Target 类型
		targets, err := targetsFromGroup(tg, sp.config)
		if err != nil {
			level.Error(sp.logger).Log("msg", "creating targets failed", "err", err)
			continue
		}

		// 将有效的 targets 添加到 all ，等待处理
		for _, t := range targets {

			// 判断 Target t 的有效 label 是否大于 0
			if t.Labels().Len() > 0 {
				// 添加到 all 队列中
				all = append(all, t)
			} else if t.DiscoveredLabels().Len() > 0 {
				// 记录无效的 targets
				sp.droppedTargets = append(sp.droppedTargets, t)
			}
		}
	}

	// 解锁
	sp.mtx.Unlock()

	// 处理 all 队列，执行 Scrape 同步操作
	sp.sync(all)

	targetSyncIntervalLength.WithLabelValues(sp.config.JobName).Observe(
		time.Since(start).Seconds(),
	)

	targetScrapePoolSyncsCounter.WithLabelValues(sp.config.JobName).Inc()
}

// sync takes a list of potentially duplicated targets, deduplicates them, starts
// scrape loops for new targets, and stops scrape loops for disappeared targets.
// It returns after all stopped scrape loops terminated.
//
//
// sp.sync() 方法对比新的 Target 列表和原来的 Target 列表，
// 若发现不在原来的 Target 列表中，则新建该 target 的 scrapeLoop ，通过协程启动 scrapeLoop 的 run 方法，并发采集存储指标;
// 然后检查原来的 Target 列表是否存在失效的 Target，若存在，则移除。
//
// 说明:
// 	1. 对 targets 去重
//	2. 遍历 Targets 列表
//    2.1 为新增的 target 启动 scrape loop
//    2.2 为消失的 target 停止 scrape loop
//
func (sp *scrapePool) sync(targets []*Target) {

	// 加解锁
	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	var (
		// targets 去重列表，每次执行 sync() 这个变量都重新创建，用来保存当前生效的所有 targets，移除旧的已失效的 targets
		uniqueTargets = map[uint64]struct{}{}
		// 采集周期
		interval = time.Duration(sp.config.ScrapeInterval)
		// 采集超时时间
		timeout = time.Duration(sp.config.ScrapeTimeout)
		limit   = int(sp.config.SampleLimit)
		// 重复 lable 是否覆盖
		honorLabels     = sp.config.HonorLabels
		honorTimestamps = sp.config.HonorTimestamps
		mrc             = sp.config.MetricRelabelConfigs
	)

	// 1. 遍历所有 targets
	for _, t := range targets {

		// 变量拷贝
		t := t
		// 生成 hash 作为 target 唯一标识
		hash := t.hash()
		// 保存
		uniqueTargets[hash] = struct{}{}

		// 1.1 检查当前 target 的 loop 是否已经在运行
		if _, ok := sp.activeTargets[hash]; !ok {

			// 若当前 target 的 loop 尚未运行

			// 1.1.1. 创建 targetScraper 对象
			s := &targetScraper{
				Target:  t,         // target
				client:  sp.client, // http client
				timeout: timeout,   // timeout
			}

			// 1.1.2. 创建 scrape loop，来收集 t 上的指标数据
			l := sp.newLoop(scrapeLoopOptions{
				target:          t,
				scraper:         s,
				limit:           limit,
				honorLabels:     honorLabels,
				honorTimestamps: honorTimestamps,
				mrc:             mrc,
			})

			// 1.1.3. 保存当前 target 到活跃列表中
			sp.activeTargets[hash] = t

			// 1.1.4. 保存当前 scrape loop 到运行 loop 列表中
			sp.loops[hash] = l

			// 1.1.5. 通过协程启动 scrapeLoop 的 run() 方法，采集和存储指标
			go l.run(interval, timeout, nil)

		} else {

		// 1.2 若当前 target 的 loop 已经运行，重置 target 的最新 labels

			// Need to keep the most updated labels information
			// for displaying it in the Service Discovery web page.
			sp.activeTargets[hash].SetDiscoveredLabels(t.DiscoveredLabels())

		}
	}



	var wg sync.WaitGroup

	// Stop and remove old targets and scraper loops.
	//
	// 检查 Target 列表是否存在失效的 Target ，若存在则移除。
	for hash := range sp.activeTargets {
		// 若 hash 不存在于 uniqueTargets 中，则其已失效。
		if _, ok := uniqueTargets[hash]; !ok {
			wg.Add(1)
			go func(l loop) {
				// 停止 target 的 loop
				l.stop()
				wg.Done()
			}(sp.loops[hash])
			delete(sp.loops, hash)
			delete(sp.activeTargets, hash)
		}
	}


	// Wait for all potentially stopped scrapers to terminate.
	// This covers the case of flapping targets.
	// If the server is under high load, a new scraper may be active and tries to insert.
	// The old scraper that didn't terminate yet could still be inserting a previous sample set.
	wg.Wait()
}


func mutateSampleLabels(lset labels.Labels, target *Target, honor bool, rc []*relabel.Config) labels.Labels {

	// 1. 把 lset 添加到 lb 中
	lb := labels.NewBuilder(lset)


	// 2. 把 target.Labels() 添加到 lb 中，根据 honor 来解决 label 名冲突（重命名或者直接覆盖）
	if honor {
		// 遍历 taget.Labels() ，若其中某个 label 不存在于 lset 中，就添加到 lset 。
		for _, l := range target.Labels() {
			if !lset.Has(l.Name) {
				lb.Set(l.Name, l.Value)
			}
		}
	} else {
		// 遍历 taget.Labels() ，
		// 若其中某个 label 不存在于 lset 中，就添加到 lset；
		// 若已存在，就将已存在的指标 label 重命名（添加固定前缀），再添加 label 便不会冲突。
		for _, l := range target.Labels() {
			// existingValue will be empty if l.Name doesn't exist.
			existingValue := lset.Get(l.Name)
			if existingValue != "" {
				lb.Set(model.ExportedLabelPrefix+l.Name, existingValue)
			}
			// It is now safe to set the target label.
			lb.Set(l.Name, l.Value)
		}
	}

	// 3. 取出处理后的 labels
	res := lb.Labels()

	// 4. 进行 relabel 处理
	if len(rc) > 0 {
		res = relabel.Process(res, rc...)
	}

	// 5. 返回
	return res
}

func mutateReportSampleLabels(lset labels.Labels, target *Target) labels.Labels {
	lb := labels.NewBuilder(lset)

	for _, l := range target.Labels() {
		lb.Set(model.ExportedLabelPrefix+l.Name, lset.Get(l.Name))
		lb.Set(l.Name, l.Value)
	}

	return lb.Labels()
}

// appender returns an appender for ingested samples from the target.
//
//
// 在提交到 storage engine 前，还有两个操作需要注意：
// 	1. timeLimitAppender
// 	2. limitAppender
//
// timeLimitAppender 是用来限制 data 的时效性的。
// 如果某一条数据在提交给 storage 进行存储的时候，生成这条数据已经超过10分钟，那么 prometheus 就会抛错。
// 目前 10 分钟是写死到代码里面的，无法通过配置文件配置。
//
// limitAppender 是用来限制存储的数据label个数，如果超过限制，该数据将被忽略，不入存储；
// 默认值为 0，表示没有限制，可以通过配置文件中的 sample_limit 来进行配置。
//
func appender(app storage.Appender, limit int) storage.Appender {


	app = &timeLimitAppender{
		Appender: app,
		maxTime:  timestamp.FromTime(time.Now().Add(maxAheadTime)),
	}


	// The limit is applied after metrics are potentially dropped via relabeling.
	if limit > 0 {
		app = &limitAppender{
			Appender: app,
			limit:    limit,
		}
	}

	return app
}



// A scraper retrieves samples and accepts a status report at the end.
//
// scraper 进行数据的抓取并在抓取结束后上报执行状态。
type scraper interface {
	// 抓取数据
	scrape(ctx context.Context, w io.Writer) (string, error)
	// 上报信息
	Report(start time.Time, dur time.Duration, err error)
	// 记录数据偏移
	offset(interval time.Duration, jitterSeed uint64) time.Duration
}


// targetScraper implements the scraper interface for a target.
//
// 说明:
// 	targetScraper 实现了 scrape 方法，Target 实现了 report 和 offset 方法，
// 	targetScraper 通过匿名包含 Target 对象实现了 scraper 接口，便可以从 Target 进行数据抓取。
//
type targetScraper struct {

	*Target

	client  *http.Client
	req     *http.Request
	timeout time.Duration

	gzipr *gzip.Reader
	buf   *bufio.Reader
}

const acceptHeader = `application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`

var userAgentHeader = fmt.Sprintf("Prometheus/%s", version.Version)

func (s *targetScraper) scrape(ctx context.Context, w io.Writer) (string, error) {


	// 构造 http 请求对象
	if s.req == nil {
		req, err := http.NewRequest("GET", s.URL().String(), nil)
		if err != nil {
			return "", err
		}
		req.Header.Add("Accept", acceptHeader)
		req.Header.Add("Accept-Encoding", "gzip")
		req.Header.Set("User-Agent", userAgentHeader)
		req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", fmt.Sprintf("%f", s.timeout.Seconds()))

		s.req = req
	}

	// 发送 http 请求
	resp, err := s.client.Do(s.req.WithContext(ctx))
	if err != nil {
		return "", err
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()


	// 返回值校验
	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("server returned HTTP status %s", resp.Status)
	}

	// 非压缩数据，直接将 rsp.Body 写入到 w 中，返回
	if resp.Header.Get("Content-Encoding") != "gzip" {
		_, err = io.Copy(w, resp.Body)
		if err != nil {
			return "", err
		}
		return resp.Header.Get("Content-Type"), nil
	}

	// 压缩数据，先解压缩，再把数据写入到 w 中
	if s.gzipr == nil {
		s.buf = bufio.NewReader(resp.Body)
		s.gzipr, err = gzip.NewReader(s.buf)
		if err != nil {
			return "", err
		}
	} else {
		s.buf.Reset(resp.Body)
		if err = s.gzipr.Reset(s.buf); err != nil {
			return "", err
		}
	}

	_, err = io.Copy(w, s.gzipr)
	s.gzipr.Close()
	if err != nil {
		return "", err
	}

	// 返回
	return resp.Header.Get("Content-Type"), nil
}


// A loop can run and be stopped again.
//
// It must not be reused after it was stopped.
type loop interface {
	run(interval, timeout time.Duration, errc chan<- error)
	stop()
	getCache() *scrapeCache
	disableEndOfRunStalenessMarkers()
}

type cacheEntry struct {

	// A reference number is returned which can be used to add further samples in the same or later transactions.
	// Returned reference numbers are ephemeral and may be rejected in calls to AddFast() at any point.
	//
	// Adding the sample via Add() returns a new reference number.
	// If the reference is 0 it must not be used for caching.

	ref      uint64 		// 添加到本地或者远程数据库的一个返回值
	lastIter uint64			// 上一个版本号
	hash     uint64			// hash 值
	lset     labels.Labels  // 包含的 labels
}



// scrapeLoop 是对单个 Target 进行指标抓取的执行单元。
//
type scrapeLoop struct {

	scraper         scraper

	l               log.Logger
	cache           *scrapeCache
	lastScrapeSize  int
	buffers         *pool.Pool
	jitterSeed      uint64
	honorTimestamps bool

	appender            func() storage.Appender
	sampleMutator       labelsMutator
	reportSampleMutator labelsMutator

	parentCtx context.Context
	ctx       context.Context
	cancel    func()
	stopped   chan struct{}

	disabledEndOfRunStalenessMarkers bool
}



// scrapeCache tracks mappings of exposed metric strings to label sets and storage references.
//
// Additionally, it tracks staleness of series between scrapes.
//
//
type scrapeCache struct {

	// Current scrape iteration.
	iter uint64 // 被缓存的迭代次数

	// How many series and metadata entries there were at the last success.
	successfulCount int

	// Parsed string to an entry with information about the actual label set and its storage reference.
	series map[string]*cacheEntry // map 类型，key 是 metric，value 是 cacheEntry 结构体。


	// Cache of dropped metric strings and their iteration.
	//
	// The iteration must be a pointer so we can update it without setting a new entry
	// with an unsafe string in addDropped().
	droppedSeries map[string]*uint64 // 缓存不合法指标

	// seriesCur and seriesPrev store the labels of series that were seen
	// in the current and previous scrape.
	// We hold two maps and swap them out to save allocations.
	seriesCur  map[uint64]labels.Labels 	// 缓存本次 scrape 的指标
	seriesPrev map[uint64]labels.Labels 	// 缓存上次 scrape 的指标

	metaMtx  sync.Mutex 					// 同步锁
	metadata map[string]*metaEntry 			// 元数据
}

// metaEntry holds meta information about a metric.
type metaEntry struct {
	lastIter uint64 // Last scrape iteration the entry was observed at.
	typ      textparse.MetricType
	help     string
	unit     string
}

func (m *metaEntry) size() int {
	// The attribute lastIter although part of the struct it is not metadata.
	return len(m.help) + len(m.unit) + len(m.typ)
}

func newScrapeCache() *scrapeCache {
	return &scrapeCache{
		series:        map[string]*cacheEntry{},
		droppedSeries: map[string]*uint64{},
		seriesCur:     map[uint64]labels.Labels{},
		seriesPrev:    map[uint64]labels.Labels{},
		metadata:      map[string]*metaEntry{},
	}
}


// 情理 scrapeCache 结构体中包含的几个 map 类型的缓存。
func (c *scrapeCache) iterDone(flushCache bool) {

	c.metaMtx.Lock()
	count := len(c.series) + len(c.droppedSeries) + len(c.metadata)
	c.metaMtx.Unlock()


	if flushCache {
		c.successfulCount = count
	} else if count > c.successfulCount*2+1000 {
		// If a target had varying labels in scrapes that ultimately failed,
		// the caches would grow indefinitely. Force a flush when this happens.
		// We use the heuristic that this is a doubling of the cache size
		// since the last scrape, and allow an additional 1000 in case
		// initial scrapes all fail.
		flushCache = true
		targetScrapeCacheFlushForced.Inc()
	}


	if flushCache {
		// All caches may grow over time through series churn
		// or multiple string representations of the same metric. Clean up entries
		// that haven't appeared in the last scrape.


		// 保留最近两个版本的合法的 series
		for s, e := range c.series {
			if c.iter != e.lastIter {
				delete(c.series, s)
			}
		}

		// 保留最近两个版本的 droppedSeries
		for s, iter := range c.droppedSeries {
			if c.iter != *iter {
				delete(c.droppedSeries, s)
			}
		}

		c.metaMtx.Lock()

		// 保留最近十个版本的 metadata
		for m, e := range c.metadata {
			// Keep metadata around for 10 scrapes after its metric disappeared.
			if c.iter-e.lastIter > 10 {
				delete(c.metadata, m)
			}
		}
		c.metaMtx.Unlock()

		// 迭代版本号自增
		c.iter++
	}



	// Swap current and previous series.
	//
	// 把上次采集的指标集合和本次采集的指标集合互换
	c.seriesPrev, c.seriesCur = c.seriesCur, c.seriesPrev



	// We have to delete every single key in the map.
	//
	// 清空本次采集的指标集合，以备下次迭代使用
	for k := range c.seriesCur {
		delete(c.seriesCur, k)
	}

}


// 根据指标信息获取缓存结构体
func (c *scrapeCache) get(met string) (*cacheEntry, bool) {

	// series 是 map 类型，key 是 metric，value 是结构体 cacheEntry 指针
	e, ok := c.series[met]
	if !ok {
		return nil, false
	}

	e.lastIter = c.iter
	return e, true
}


// 添加 metric 的缓存结构体 cacheEntry
func (c *scrapeCache) addRef(met string, ref uint64, lset labels.Labels, hash uint64) {

	if ref == 0 {
		return
	}

	// series 是 map 类型，key 为 metric ，value 是结构体 cacheEntry
	c.series[met] = &cacheEntry{
		ref: ref,
		lastIter: c.iter,
		lset: lset,
		hash: hash,
	}
}


// 添加无效指标到 map 类型的 droppedSeries 中
func (c *scrapeCache) addDropped(met string) {
	iter := c.iter
	// droppedSeries 是 map 类型，以 metric 作为 key，版本作为 value。
	c.droppedSeries[met] = &iter
}

// 判断指标的合法性
func (c *scrapeCache) getDropped(met string) bool {
	// 判断 metric 是否在非法的 dropperSeries 的 map 类型里，key 是 metric，value 是迭代版本号。
	iterp, ok := c.droppedSeries[met]
	if ok {
		*iterp = c.iter
	}
	return ok
}

// 添加不带时间戳的指标到 map 类型的 seriesCur 中；以 metric lset 的 hash 值作为唯一标识。
func (c *scrapeCache) trackStaleness(hash uint64, lset labels.Labels) {
	c.seriesCur[hash] = lset
}

// 比较两个 map：seriesCur 和 seriesPrev ，查找过期指标。
func (c *scrapeCache) forEachStale(f func(labels.Labels) bool) {
	// 如果 seriesPrev 中的指标(label)存在于 seriesCur ，则不处理，如果不存在，则说明过期。
	for h, lset := range c.seriesPrev {
		if _, ok := c.seriesCur[h]; !ok {
			if !f(lset) {
				break
			}
		}
	}
}

func (c *scrapeCache) setType(metric []byte, t textparse.MetricType) {
	c.metaMtx.Lock()

	e, ok := c.metadata[yoloString(metric)]
	if !ok {
		e = &metaEntry{
			typ: textparse.MetricTypeUnknown,
		}
		c.metadata[string(metric)] = e
	}
	e.typ = t
	e.lastIter = c.iter

	c.metaMtx.Unlock()
}

func (c *scrapeCache) setHelp(metric, help []byte) {
	c.metaMtx.Lock()

	e, ok := c.metadata[yoloString(metric)]
	if !ok {
		e = &metaEntry{typ: textparse.MetricTypeUnknown}
		c.metadata[string(metric)] = e
	}
	if e.help != yoloString(help) {
		e.help = string(help)
	}
	e.lastIter = c.iter

	c.metaMtx.Unlock()
}

func (c *scrapeCache) setUnit(metric, unit []byte) {
	c.metaMtx.Lock()

	e, ok := c.metadata[yoloString(metric)]
	if !ok {
		e = &metaEntry{typ: textparse.MetricTypeUnknown}
		c.metadata[string(metric)] = e
	}
	if e.unit != yoloString(unit) {
		e.unit = string(unit)
	}
	e.lastIter = c.iter

	c.metaMtx.Unlock()
}

func (c *scrapeCache) GetMetadata(metric string) (MetricMetadata, bool) {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()

	m, ok := c.metadata[metric]
	if !ok {
		return MetricMetadata{}, false
	}
	return MetricMetadata{
		Metric: metric,
		Type:   m.typ,
		Help:   m.help,
		Unit:   m.unit,
	}, true
}

func (c *scrapeCache) ListMetadata() []MetricMetadata {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()

	res := make([]MetricMetadata, 0, len(c.metadata))

	for m, e := range c.metadata {
		res = append(res, MetricMetadata{
			Metric: m,
			Type:   e.typ,
			Help:   e.help,
			Unit:   e.unit,
		})
	}
	return res
}

// MetadataSize returns the size of the metadata cache.
func (c *scrapeCache) SizeMetadata() (s int) {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()
	for _, e := range c.metadata {
		s += e.size()
	}

	return s
}

// MetadataLen returns the number of metadata entries in the cache.
func (c *scrapeCache) LengthMetadata() int {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()

	return len(c.metadata)
}

func newScrapeLoop(ctx context.Context,
	sc scraper,
	l log.Logger,
	buffers *pool.Pool,
	sampleMutator labelsMutator,
	reportSampleMutator labelsMutator,
	appender func() storage.Appender,
	cache *scrapeCache,
	jitterSeed uint64,
	honorTimestamps bool,
) *scrapeLoop {


	if l == nil {
		l = log.NewNopLogger()
	}

	if buffers == nil {
		buffers = pool.New(1e3, 1e6, 3, func(sz int) interface{} { return make([]byte, 0, sz) })
	}

	if cache == nil {
		cache = newScrapeCache()
	}

	sl := &scrapeLoop{
		scraper:             sc,
		buffers:             buffers,
		cache:               cache,
		appender:            appender,
		sampleMutator:       sampleMutator,
		reportSampleMutator: reportSampleMutator,
		stopped:             make(chan struct{}),
		jitterSeed:          jitterSeed,
		l:                   l,
		parentCtx:           ctx,
		honorTimestamps:     honorTimestamps,
	}

	sl.ctx, sl.cancel = context.WithCancel(ctx)

	return sl
}


//
//
// 执行 http/https Get 请求到 targets 的 metricPath 拉取监控数据缓存到 buf，
// 然后将时序数据 append 到 TSDB，存储的时间戳 timestamp 是抓取监控指标的时间(正常情况下严格递增)。


//
//
//
// scrapeLoop.run() 把接口成员 scraper 抽象出来的三个接口都进行了调用：
// 	1. scraper.offset() 用于控制第一次执行的时候等待的间隔
// 	2. scraper.Scrape() 进行数据的抓取
// 	3. scraper.report() 上报执行状态
//



// run() 方法主要实现两个功能：指标采集(scrape)和指标存储。
// 此外，为了实现对象的复用，在采集(scrape)过程中，使用了 sync.Pool 机制提高性能，即每次采集(scrape)完成后，
// 都会申请和本次采集(scrape)指标存储空间一样的大小的 bytes ，加入到 buffer 中，以备下次指标采集(scrape)直接使用。
//
//
// [!!!]
func (sl *scrapeLoop) run(interval, timeout time.Duration, errc chan<- error) {

	select {
	// 检测超时
	case <-time.After(sl.scraper.offset(interval, sl.jitterSeed)):
		// Continue after a scraping offset.
	// 停止，退出
	case <-sl.ctx.Done():
		close(sl.stopped)
		return
	}

	var last time.Time
	// 设置定时器
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

mainLoop:

	for {

		select {
		case <-sl.parentCtx.Done():
			close(sl.stopped)
			return
		case <-sl.ctx.Done():
			break mainLoop
		default:
		}


		var (
			// 抓取时间，作为 metrics 的 timestamp 存储到 storage 中。
			start             = time.Now()
			scrapeCtx, cancel = context.WithTimeout(sl.ctx, timeout)
		)


		// Only record after the first scrape.
		//
		// 如果非首次抓取，则上报相邻两次执行的时间间隔（秒）
		if !last.IsZero() {
			targetIntervalLength.WithLabelValues(interval.String()).Observe(
				time.Since(last).Seconds(),
			)
		}

		// 在执行 scrape 前，向 mem pool 申请和上一次 scrape 结果一样大小的 byte slice，
		// 并封装成 byte buffer 供 scraper.scrape() 存储数据使用。
		b := sl.buffers.Get(sl.lastScrapeSize).([]byte)
		buf := bytes.NewBuffer(b)

		// 抓取 metrics 存储到 buf 中
		contentType, scrapeErr := sl.scraper.scrape(scrapeCtx, buf)

		// 结束抓取
		cancel()

		// 如果抓取成功，设置 b 为 buf，并更新 sl.lastScrapeSize 变量
		if scrapeErr == nil {

			b = buf.Bytes()
			// NOTE: There were issues with misbehaving clients in the past
			// that occasionally returned empty results. We don't want those
			// to falsely reset our buffer size.
			if len(b) > 0 {
				// 存储本次抓取所占用的磁盘空间，留待下次抓取时参考
				sl.lastScrapeSize = len(b)
			}

		// 如果抓取失败，打印错误日志并传递错误信息到 errc 中，此时 b 为 nil
		} else {
			level.Debug(sl.l).Log("msg", "Scrape failed", "err", scrapeErr.Error())
			if errc != nil {
				errc <- scrapeErr
			}
		}

		// A failed scrape is the same as an empty scrape,
		// we still call sl.append to trigger stale markers.




		// 通过 sl.scraper.scrape() 完成指标抓取后，将数据交给 append 函数。
		// append 函数主要在将数据交给 storage engine 前，做一些预处理的工作。
		//
		// 在预处理工作中，prometheus 设计了 scrapeCache 结构，用来追踪从 metric string 到
		// label sets storage references 的对应 mapping 关系。
		//
		// 其次，scrapeCache 还会记录两次 scrape 操作之间重复的那部分数据，以便快速的判断重复的数据，
		// 对于重复的数据，就没有必要在此存储到 storage engine 中了。
		//
		// append 函数首先会把拿到的数据(b) 利用 textparse parser 解析成 met(metric的简写)变量，
		// 然后判断 met 是不是重复了，如果重复了就会丢弃这条数据。
		//
		// 下面会根据 met 从 scripeCache.series 中查找，如果查找到对应的 cacheEntity，则会调用 appender 的 AddFast 函数进行存储操作。
		//
		// 至于存储的细节，超出了 scrape 的范围，我们会在下面的文章中分析。
		//
		// 如果没有查找到对应 cacheEntity，那么会调用 appender 的 Add 方法进行存储操作，
		// 并且把相关信息存储到 cache 中，以备下一个次 scrape 的时候进行对比。
		//
		// appender 是一个接口，具体有好几种实现存储的方式。
		//
		// 类似于 mysql 中的 engine，可以把数据存储在 innoDB engine ，也可以存储在其他 mysql engine 中。



		// b 中存储了本次抓取的指标数据，start 为当前时间。
		total, added, seriesAdded, appErr := sl.append(b, contentType, start)



		if appErr != nil {
			level.Debug(sl.l).Log("msg", "Append failed", "err", appErr)
			// The append failed, probably due to a parse error or sample limit.
			// Call sl.append again with an empty scrape to trigger stale markers.
			if _, _, _, err := sl.append([]byte{}, "", start); err != nil {
				level.Warn(sl.l).Log("msg", "Append failed", "err", err)
			}
		}

		// b 已经使用完毕，放回 mem pool 中
		sl.buffers.Put(b)

		if scrapeErr == nil {
			scrapeErr = appErr
		}

		// 状态上报
		if err := sl.report(start, time.Since(start), total, added, seriesAdded, scrapeErr); err != nil {
			level.Warn(sl.l).Log("msg", "Appending scrape report failed", "err", err)
		}

		// 设置 last
		last = start

		// 停止 scrapeLoop
		select {
		case <-sl.parentCtx.Done():
			close(sl.stopped)
			return
		case <-sl.ctx.Done():
			break mainLoop
		case <-ticker.C:
		}

	}

	close(sl.stopped)

	if !sl.disabledEndOfRunStalenessMarkers {
		sl.endOfRunStaleness(last, ticker, interval)
	}
}

func (sl *scrapeLoop) endOfRunStaleness(last time.Time, ticker *time.Ticker, interval time.Duration) {

	// Scraping has stopped.
	//
	// We want to write stale markers but the target may be recreated,
	// so we wait just over 2 scrape intervals before creating them.
	//
	// If the context is canceled, we presume the server is shutting down
	// and will restart where is was. We do not attempt to write stale markers in this case.


	if last.IsZero() {
		// There never was a scrape, so there will be no stale markers.
		return
	}


	// Wait for when the next scrape would have been, record its timestamp.
	var staleTime time.Time
	select {
	case <-sl.parentCtx.Done():
		return
	case <-ticker.C:
		staleTime = time.Now()
	}

	// Wait for when the next scrape would have been, if the target was recreated
	// samples should have been ingested by now.
	select {
	case <-sl.parentCtx.Done():
		return
	case <-ticker.C:
	}

	// Wait for an extra 10% of the interval, just to be safe.
	select {
	case <-sl.parentCtx.Done():
		return
	case <-time.After(interval / 10):
	}

	// Call sl.append again with an empty scrape to trigger stale markers.
	// If the target has since been recreated and scraped, the
	// stale markers will be out of order and ignored.
	if _, _, _, err := sl.append([]byte{}, "", staleTime); err != nil {
		level.Error(sl.l).Log("msg", "stale append failed", "err", err)
	}
	if err := sl.reportStale(staleTime); err != nil {
		level.Error(sl.l).Log("msg", "stale report failed", "err", err)
	}
}


// Stop the scraping. May still write data and stale markers after it has returned.
// Cancel the context to stop all writes.
func (sl *scrapeLoop) stop() {
	sl.cancel()
	<-sl.stopped
}

func (sl *scrapeLoop) disableEndOfRunStalenessMarkers() {
	sl.disabledEndOfRunStalenessMarkers = true
}

func (sl *scrapeLoop) getCache() *scrapeCache {
	return sl.cache
}

type appendErrors struct {
	numOutOfOrder  int
	numDuplicates  int
	numOutOfBounds int
}












// 整个存储逻辑都围绕着过滤无效指标进行。
//
// 存储的时候指标分为有时间戳与无时间戳两种情况。
//
//（1）有时间戳：
//
//	a. 解析指标数据通过 Series()
//	b. 利用 getDropped 判断指标是否有效，无效则跳出处理
//	c. 通过 get 查找对应cacheEntry，如果找到利用 app.AddFast 直接存储样本值。
//	   如果未找到，使用 sampleMutator 进行解析重置，判断 lset 是否为空，为空则使用 addDropped 添加到无效字典中，跳出当前处理，
//	   如果有效则使用 app.Add 存储指标。(可以看到，通过 get 找到使用 AddFast 存储，未找到使用 Add 存储，感兴趣可以看下两个fun实现的区别)。
//	d. 通过 forEachStale 检查指标是否过期。
//	e. app.Add 标记过期指标
//	f. 调用 iterDone 进行相关缓存清理。
//
//（2）无时间戳：
//	a. 每次存储后，如果不带时间戳都会调用 trackStaleness ，存储指标到 seriesCur 中。





// append() 方法利用 scrapeCache 的成员方法，实现对指标的合法性验证、过滤及存储。
//
// 其中，存储路径分两条：
// (1) 如果能在 scrapeCache 中找到该 metric 的 cacheEntry，说明之前添加过，则做 AddFast 存储路径；
// (2) 如果不能，需要生成指标的 lables、hash 及 ref 等，通过 Add 方法进行存储, 然后把该指标的 cacheEntry 加到 scrapeCache 中。
func (sl *scrapeLoop) append(b []byte, contentType string, ts time.Time) (total, added, seriesAdded int, err error) {

	var (
		app            = sl.appender()					// 获取指标存储器(本地或远端)，在服务启动的时候已经指定
		p              = textparse.New(b, contentType) 	// 创建指标解析器，参数 b 包含当次收集的所有指标
		defTime        = timestamp.FromTime(ts)			// 将 ts 从 time.Time 转换为 整数时间戳
		appErrs        = appendErrors{}					// 错误计数器
		sampleLimitErr error 							// 错误
	)

	defer func() {

		// 若出错，则存储器做回滚操作
		if err != nil {
			app.Rollback()
			return
		}

		// 否则，存储器做提交处理
		if err = app.Commit(); err != nil {
			return
		}

		// Only perform cache cleaning if the scrape was not empty.
		// An empty scrape (usually) is used to indicate a failed scrape.
		//
		// 执行缓存清理相关工作
		sl.cache.iterDone(len(b) > 0)

	}()

loop:

	for {

		var (
			et          textparse.Entry
			sampleAdded bool
		)

		// 逐条从 b 中读取 Entry，返回 EntryType et
		if et, err = p.Next(); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		// 以下几个 Entry Type 跳过
		switch et {
		case textparse.EntryType:
			sl.cache.setType(p.Type())
			continue
		case textparse.EntryHelp:
			sl.cache.setHelp(p.Help())
			continue
		case textparse.EntryUnit:
			sl.cache.setUnit(p.Unit())
			continue
		case textparse.EntryComment:
			continue
		default:
		}

		// 总指标数++
		total++

		// 设置当前指标的时间戳为采集的时间戳
		t := defTime

		// 读取当前指标，返回 metric, timestamp, value
		met, tp, v := p.Series()

		// 如果设置了 honorTimestamps，时间戳 tp 设置为 nil
		if !sl.honorTimestamps {
			tp = nil
		}

		// 如果时间戳不为空，更新当前 t
		if tp != nil {
			t = *tp
		}

		// 检查指标 met 是否有效，无效则直接跳过
		if sl.cache.getDropped(yoloString(met)) {
			continue
		}

		// 根据指标 met 获取对应的 cacheEntry 结构
		ce, ok := sl.cache.get(yoloString(met))

		// 如果能从缓存中获取，则执行指标的存储操作
		if ok {

			// 指标存储
			err = app.AddFast(ce.ref, t, v)

			// 错误检查
			sampleAdded, err = sl.checkAddError(ce, met, tp, err, &sampleLimitErr, appErrs)

			// In theory this should never happen.
			if err == storage.ErrNotFound {
				ok = false
			}
		}

		// 如果在缓存中未查找到（或者在上一步存储指标时发生 storage.ErrNotFound 错误）

		if !ok {



			// [!!!]
			// 在 prometheus storage 中，metric name 被当作特殊标签 "__name__" 对待，一个 metric 等价于一个有序的 labels 数组。
			// 这样，对 metric 的操作都统一成对 labels 的处理。这也便解释了，为什么 storage.Add(l labels.Labels, ...) 方法的
			// 第一个参数是 labels.Labels 对象。

			// p.Metric(）函数从一个 metric 的字节序列里解析成一组 labels 键值对，这组 labels 即代表该 metric 。
			var lset labels.Labels
			mets := p.Metric(&lset)


			// 根据 lset 计算当前 metric 的 hash 值
			hash := lset.Hash()



			// Hash label set as it is seen local to the target.
			// Then add target labels and relabeling and store the final label set.
			//
			// 根据 sp.config.HonorLabels 和 sp.config.MetricRelabelConfigs 规则，对指标的 lset 中的 labels 进行重置。
			lset = sl.sampleMutator(lset)



			// The label set may be set to nil to indicate dropping.
			//
			// 若重置后的 labels 为空，则表明该 mets 应该丢弃，把该指标加到 droppedSeries 中
			if lset == nil {
				sl.cache.addDropped(mets)
				continue
			}



			// 若当前 metric 不包含 "__name__" 标签，为非法标签，出错退出
			if !lset.Has(labels.MetricName) {
				err = errNameLabelMandatory
				break loop
			}


			var ref uint64
			// 存储当前指标 lset、timestamp、value 到 storage 中，返回值 ref 用于构造缓存 cacheEntry{} 对象
			ref, err = app.Add(lset, t, v)


			// 错误检查&处理
			sampleAdded, err = sl.checkAddError(nil, met, tp, err, &sampleLimitErr, appErrs)
			if err != nil {
				if err != storage.ErrNotFound {
					level.Debug(sl.l).Log("msg", "Unexpected error", "series", string(met), "err", err)
				}
				break loop
			}



			// 如果时间戳为空，会调用 sl.cache.trackStaleness 将当前指标 hash => lset 存储到 seriesCur 中。
			if tp == nil {
				// Bypass staleness logic if there is an explicit timestamp.
				sl.cache.trackStaleness(hash, lset)
			}




			// 缓存指标的 cacheEntry 到 sl.cache 中
			sl.cache.addRef(mets, ref, lset, hash)




			// 增加缓存计数
			if sampleAdded && sampleLimitErr == nil {
				seriesAdded++
			}



		}





		// Increment added even if there's a sampleLimitErr so we correctly report the number of samples scraped.
		if sampleAdded || sampleLimitErr != nil {
			added++
		}




	}


	// 错误相关处理，不做分析。
	if sampleLimitErr != nil {


		if err == nil {
			err = sampleLimitErr
		}


		// We only want to increment this once per scrape, so this is Inc'd outside the loop.
		targetScrapeSampleLimit.Inc()


	}



	if appErrs.numOutOfOrder > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting out-of-order samples", "num_dropped", appErrs.numOutOfOrder)
	}
	if appErrs.numDuplicates > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting samples with different value but same timestamp", "num_dropped", appErrs.numDuplicates)
	}
	if appErrs.numOutOfBounds > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "num_dropped", appErrs.numOutOfBounds)
	}


	if err == nil {

		// 过期指标检查 & 处理
		sl.cache.forEachStale(
			// 过期指标处理函数
			func(lset labels.Labels) bool {
				// Series no longer exposed, mark it stale.

				// 调用 app.Add() 将 lset 的指标值设为 StaleNaN
				_, err = app.Add(lset, defTime, math.Float64frombits(value.StaleNaN))
				// 错误处理
				switch errors.Cause(err) {
				case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp: // 这些错误不做处理
					// Do not count these in logging, as this is expected if a target
					// goes away and comes back again with a new scrape loop.
					err = nil
				}
				return err == nil // true or false
			},
		)
	}

	return
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}

// Adds samples to the appender, checking the error, and then returns the # of samples added,
// whether the caller should continue to process more samples, and any sample limit errors.

func (sl *scrapeLoop) checkAddError(ce *cacheEntry, met []byte, tp *int64, err error, sampleLimitErr *error, appErrs appendErrors) (bool, error) {


	switch errors.Cause(err) {
	case nil:
		// 如果不带时间戳
		if tp == nil && ce != nil {
			// 存储该不带时间戳的指标到seriesCur中。
			sl.cache.trackStaleness(ce.hash, ce.lset)
		}
		return true, nil
	// 未找到
	case storage.ErrNotFound:
		return false, storage.ErrNotFound

	// 样本乱序
	case storage.ErrOutOfOrderSample:
		// 样本乱序错误数++、打印日志、上报
		appErrs.numOutOfOrder++
		level.Debug(sl.l).Log("msg", "Out of order sample", "series", string(met))
		targetScrapeSampleOutOfOrder.Inc()
		return false, nil
	// 样本重复
	case storage.ErrDuplicateSampleForTimestamp:
		// 样本重复错误数++、打印日志、上报
		appErrs.numDuplicates++
		level.Debug(sl.l).Log("msg", "Duplicate sample for timestamp", "series", string(met))
		targetScrapeSampleDuplicate.Inc()
		return false, nil
	// 存储越界
	case storage.ErrOutOfBounds:
		// 存储越界错误数++、打印日志、上报
		appErrs.numOutOfBounds++
		level.Debug(sl.l).Log("msg", "Out of bounds metric", "series", string(met))
		targetScrapeSampleOutOfBounds.Inc()
		return false, nil
	// 超出样本限制
	case errSampleLimit:
		// Keep on parsing output if we hit the limit, so we report the correct total number of samples scraped.
		//
		// 如果我们达到上限也要继续解析输出，所以我们要上报正确的样本总量
		*sampleLimitErr = err
		return false, nil
	// 未知情况
	default:
		return false, err
	}
}

// The constants are suffixed with the invalid \xff unicode rune to avoid collisions
// with scraped metrics in the cache.
const (
	scrapeHealthMetricName       = "up" + "\xff"
	scrapeDurationMetricName     = "scrape_duration_seconds" + "\xff"
	scrapeSamplesMetricName      = "scrape_samples_scraped" + "\xff"
	samplesPostRelabelMetricName = "scrape_samples_post_metric_relabeling" + "\xff"
	scrapeSeriesAddedMetricName  = "scrape_series_added" + "\xff"
)

func (sl *scrapeLoop) report(start time.Time, duration time.Duration, scraped, appended, seriesAdded int, scrapeErr error) (err error) {
	sl.scraper.Report(start, duration, scrapeErr)

	ts := timestamp.FromTime(start)

	var health float64
	if scrapeErr == nil {
		health = 1
	}
	app := sl.appender()
	defer func() {
		if err != nil {
			app.Rollback()
			return
		}
		err = app.Commit()
	}()

	if err = sl.addReportSample(app, scrapeHealthMetricName, ts, health); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeDurationMetricName, ts, duration.Seconds()); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeSamplesMetricName, ts, float64(scraped)); err != nil {
		return
	}
	if err = sl.addReportSample(app, samplesPostRelabelMetricName, ts, float64(appended)); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeSeriesAddedMetricName, ts, float64(seriesAdded)); err != nil {
		return
	}
	return
}

func (sl *scrapeLoop) reportStale(start time.Time) (err error) {
	ts := timestamp.FromTime(start)
	app := sl.appender()
	defer func() {
		if err != nil {
			app.Rollback()
			return
		}
		err = app.Commit()
	}()

	stale := math.Float64frombits(value.StaleNaN)

	if err = sl.addReportSample(app, scrapeHealthMetricName, ts, stale); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeDurationMetricName, ts, stale); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeSamplesMetricName, ts, stale); err != nil {
		return
	}
	if err = sl.addReportSample(app, samplesPostRelabelMetricName, ts, stale); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeSeriesAddedMetricName, ts, stale); err != nil {
		return
	}
	return
}

func (sl *scrapeLoop) addReportSample(app storage.Appender, s string, t int64, v float64) error {
	ce, ok := sl.cache.get(s)
	if ok {
		err := app.AddFast(ce.ref, t, v)
		switch errors.Cause(err) {
		case nil:
			return nil
		case storage.ErrNotFound:
			// Try an Add.
		case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
			// Do not log here, as this is expected if a target goes away and comes back
			// again with a new scrape loop.
			return nil
		default:
			return err
		}
	}
	lset := labels.Labels{
		// The constants are suffixed with the invalid \xff unicode rune to avoid collisions
		// with scraped metrics in the cache.
		// We have to drop it when building the actual metric.
		labels.Label{Name: labels.MetricName, Value: s[:len(s)-1]},
	}

	hash := lset.Hash()
	lset = sl.reportSampleMutator(lset)

	ref, err := app.Add(lset, t, v)
	switch errors.Cause(err) {
	case nil:
		sl.cache.addRef(s, ref, lset, hash)
		return nil
	case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
		return nil
	default:
		return err
	}
}

// zeroConfig returns a new scrape config that only contains configuration items
// that alter metrics.
func zeroConfig(c *config.ScrapeConfig) *config.ScrapeConfig {
	z := *c
	// We zero out the fields that for sure don't affect scrape.
	z.ScrapeInterval = 0
	z.ScrapeTimeout = 0
	z.SampleLimit = 0
	z.HTTPClientConfig = config_util.HTTPClientConfig{}
	return &z
}

// reusableCache compares two scrape config and tells whether the cache is still valid.
func reusableCache(r, l *config.ScrapeConfig) bool {
	if r == nil || l == nil {
		return false
	}
	return reflect.DeepEqual(zeroConfig(r), zeroConfig(l))
}
