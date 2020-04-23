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

package remote

import (
	"context"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/blastbao/prometheus/config"
	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/pkg/relabel"
	"github.com/blastbao/prometheus/prompb"
	"github.com/blastbao/prometheus/tsdb/record"
	"github.com/blastbao/prometheus/tsdb/wal"
	"github.com/prometheus/client_golang/prometheus"
)

const (


	// We track samples in/out and how long pushes take using an Exponentially Weighted Moving Average.
	ewmaWeight          = 0.2
	shardUpdateDuration = 10 * time.Second

	// Allow 30% too many shards before scaling down.
	shardToleranceFraction = 0.3
)

type queueManagerMetrics struct {
	succeededSamplesTotal     *prometheus.CounterVec
	failedSamplesTotal        *prometheus.CounterVec
	retriedSamplesTotal       *prometheus.CounterVec
	droppedSamplesTotal       *prometheus.CounterVec
	enqueueRetriesTotal       *prometheus.CounterVec
	sentBatchDuration         *prometheus.HistogramVec
	queueHighestSentTimestamp *prometheus.GaugeVec
	queuePendingSamples       *prometheus.GaugeVec
	shardCapacity             *prometheus.GaugeVec
	numShards                 *prometheus.GaugeVec
	maxNumShards              *prometheus.GaugeVec
	minNumShards              *prometheus.GaugeVec
	desiredNumShards          *prometheus.GaugeVec
	bytesSent                 *prometheus.CounterVec
}

func newQueueManagerMetrics(r prometheus.Registerer) *queueManagerMetrics {


	m := &queueManagerMetrics{}

	m.succeededSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "succeeded_samples_total",
			Help:      "Total number of samples successfully sent to remote storage.",
		},
		[]string{remoteName, endpoint},
	)

	m.failedSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "failed_samples_total",
			Help:      "Total number of samples which failed on send to remote storage, non-recoverable errors.",
		},
		[]string{remoteName, endpoint},
	)


	m.retriedSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "retried_samples_total",
			Help:      "Total number of samples which failed on send to remote storage but were retried because the send error was recoverable.",
		},
		[]string{remoteName, endpoint},
	)


	m.droppedSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "dropped_samples_total",
			Help:      "Total number of samples which were dropped after being read from the WAL before being sent via remote write.",
		},
		[]string{remoteName, endpoint},
	)


	m.enqueueRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "enqueue_retries_total",
			Help:      "Total number of times enqueue has failed because a shards queue was full.",
		},
		[]string{remoteName, endpoint},
	)


	m.sentBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "sent_batch_duration_seconds",
			Help:      "Duration of sample batch send calls to the remote storage.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{remoteName, endpoint},
	)


	m.queueHighestSentTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_highest_sent_timestamp_seconds",
			Help:      "Timestamp from a WAL sample, the highest timestamp successfully sent by this queue, in seconds since epoch.",
		},
		[]string{remoteName, endpoint},
	)


	m.queuePendingSamples = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "pending_samples",
			Help:      "The number of samples pending in the queues shards to be sent to the remote storage.",
		},
		[]string{remoteName, endpoint},
	)


	m.shardCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "shard_capacity",
			Help:      "The capacity of each shard of the queue used for parallel sending to the remote storage.",
		},
		[]string{remoteName, endpoint},
	)


	m.numShards = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "shards",
			Help:      "The number of shards used for parallel sending to the remote storage.",
		},
		[]string{remoteName, endpoint},
	)


	m.maxNumShards = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "shards_max",
			Help:      "The maximum number of shards that the queue is allowed to run.",
		},
		[]string{remoteName, endpoint},
	)


	m.minNumShards = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "shards_min",
			Help:      "The minimum number of shards that the queue is allowed to run.",
		},
		[]string{remoteName, endpoint},
	)


	m.desiredNumShards = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "shards_desired",
			Help:      "The number of shards that the queues shard calculation wants to run based on the rate of samples in vs. samples out.",
		},
		[]string{remoteName, endpoint},
	)


	m.bytesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "sent_bytes_total",
			Help:      "The total number of bytes sent by the queue.",
		},
		[]string{remoteName, endpoint},
	)


	if r != nil {
		r.MustRegister(
			m.succeededSamplesTotal,		// 成功数
			m.failedSamplesTotal,			// 失败数
			m.retriedSamplesTotal, 			// 重试数
			m.droppedSamplesTotal,			// 丢弃数
			m.enqueueRetriesTotal,			//
			m.sentBatchDuration,           	// 发送延迟
			m.queueHighestSentTimestamp, 	//
			m.queuePendingSamples,
			m.shardCapacity,
			m.numShards,
			m.maxNumShards,
			m.minNumShards,
			m.desiredNumShards,
			m.bytesSent,
		)
	}
	return m
}



// StorageClient defines an interface for sending a batch of samples to an external timeseries database.
// StorageClient 定义了一个接口，用于将一批样本发送到外部时序数据库。
type StorageClient interface {

	// Store stores the given samples in the remote storage.
	// Store 将给定的样本存储在远程存储器中。
	Store(context.Context, []byte) error

	// Name uniquely identifies the remote storage.
	// Name 唯一标识远程存储。
	Name() string

	// Endpoint is the remote read or write endpoint for the storage client.
	// Endpoint 是远程读或写实例。
	Endpoint() string
}





// QueueManager manages a queue of samples to be sent to the Storage
// indicated by the provided StorageClient.
//
//
// Implements writeTo interface used by WAL Watcher.
//
//
type QueueManager struct {



	// https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	lastSendTimestamp int64


	logger         log.Logger
	flushDeadline  time.Duration
	cfg            config.QueueConfig
	externalLabels labels.Labels
	relabelConfigs []*relabel.Config
	client         StorageClient
	watcher        *wal.Watcher


	seriesMtx            sync.Mutex

	// 保存了 ref 到 labels 的映射
	seriesLabels         map[uint64]labels.Labels

	//
	seriesSegmentIndexes map[uint64]int

	//
	droppedSeries        map[uint64]struct{}


	//
	shards      *shards
	numShards   int

	reshardChan chan int


	quit        chan struct{}
	wg          sync.WaitGroup




	samplesIn,
	samplesDropped,
	samplesOut,
	samplesOutDuration *ewmaRate

	metrics                    *queueManagerMetrics
	highestSentTimestampMetric *maxGauge
	pendingSamplesMetric       prometheus.Gauge
	enqueueRetriesMetric       prometheus.Counter
	droppedSamplesTotal        prometheus.Counter
	numShardsMetric            prometheus.Gauge
	failedSamplesTotal         prometheus.Counter
	sentBatchDuration          prometheus.Observer
	succeededSamplesTotal      prometheus.Counter
	retriedSamplesTotal        prometheus.Counter
	shardCapacity              prometheus.Gauge
	maxNumShards               prometheus.Gauge
	minNumShards               prometheus.Gauge
	desiredNumShards           prometheus.Gauge
	bytesSent                  prometheus.Counter
}





// NewQueueManager builds a new QueueManager.
func NewQueueManager(

	metrics *queueManagerMetrics,
	watcherMetrics *wal.WatcherMetrics,
	readerMetrics *wal.LiveReaderMetrics,
	logger log.Logger,

	walDir string,
	samplesIn *ewmaRate,
	cfg config.QueueConfig,
	externalLabels labels.Labels,
	relabelConfigs []*relabel.Config,
	client StorageClient,
	flushDeadline time.Duration,

	) *QueueManager {

	if logger == nil {
		logger = log.NewNopLogger()
	}


	logger = log.With(logger, remoteName, client.Name(), endpoint, client.Endpoint())


	t := &QueueManager{
		logger:         logger,
		flushDeadline:  flushDeadline,
		cfg:            cfg,
		externalLabels: externalLabels,
		relabelConfigs: relabelConfigs,
		client:         client,

		seriesLabels:         make(map[uint64]labels.Labels),
		seriesSegmentIndexes: make(map[uint64]int),
		droppedSeries:        make(map[uint64]struct{}),

		numShards:   cfg.MinShards,
		reshardChan: make(chan int),
		quit:        make(chan struct{}),

		samplesIn:          samplesIn,
		samplesDropped:     newEWMARate(ewmaWeight, shardUpdateDuration),
		samplesOut:         newEWMARate(ewmaWeight, shardUpdateDuration),
		samplesOutDuration: newEWMARate(ewmaWeight, shardUpdateDuration),

		metrics: metrics,
	}

	// 创建 wal watcher
	t.watcher = wal.NewWatcher(watcherMetrics, readerMetrics, logger, client.Name(), t, walDir)


	// 创建 shards
	t.shards = t.newShards()


	return t
}







// Append queues a sample to be sent to the remote storage.
// Blocks until all samples are enqueued on their shards or a shutdown signal is received.
//
//
//
//
func (t *QueueManager) Append(samples []record.RefSample) bool {

outer:

	// 遍历样本
	for _, s := range samples {

		t.seriesMtx.Lock()

		// 根据 s.Ref 取出关联的 labels
		lbls, ok := t.seriesLabels[s.Ref]

		// 如果未找到，则丢弃当前样本
		if !ok {

			// 上报 `丢弃样本数`
			t.droppedSamplesTotal.Inc()
			t.samplesDropped.incr(1)

			if _, ok := t.droppedSeries[s.Ref]; !ok {
				level.Info(t.logger).Log("msg", "Dropped sample for series that was not explicitly dropped via relabelling", "ref", s.Ref)
			}

			t.seriesMtx.Unlock()

			continue
		}

		t.seriesMtx.Unlock()

		// This will only loop if the queues are being resharded.
		backoff := t.cfg.MinBackoff

		// 把当前样本 s 传送给 t.shards，不断重试
		for {

			select {
			case <-t.quit:
				return false
			default:
			}

			// 样本入队
			if t.shards.enqueue(s.Ref, sample{
				labels: lbls,
				t:      s.T,
				v:      s.V,
			}) {
				// 入队成功，则继续处理下一个样本
				continue outer
			}

			// 入队失败，则上报、等待、重试...
			t.enqueueRetriesMetric.Inc()
			time.Sleep(time.Duration(backoff))
			backoff = backoff * 2
			if backoff > t.cfg.MaxBackoff {
				backoff = t.cfg.MaxBackoff
			}
		}
	}
	return true
}


// Start the queue manager sending samples to the remote storage.
//
// Does not block.
//
//
//
// Start() 方法的主要逻辑是启动了四个任务。
//
func (t *QueueManager) Start() {

	// 获取 t.client 信息
	name := t.client.Name()		// 远程存储名
	ep := t.client.Endpoint()	// 远程存储地址


	// Setup the QueueManagers metrics.
	//
	// We do this here rather than in the constructor because of the ordering of creating Queue Managers's,
	// stopping them, and then starting new ones in storage/remote/storage.go ApplyConfig.
	t.highestSentTimestampMetric = &maxGauge{
		Gauge: t.metrics.queueHighestSentTimestamp.WithLabelValues(name, ep),
	}
	t.pendingSamplesMetric = t.metrics.queuePendingSamples.WithLabelValues(name, ep)
	t.enqueueRetriesMetric = t.metrics.enqueueRetriesTotal.WithLabelValues(name, ep)
	t.droppedSamplesTotal = t.metrics.droppedSamplesTotal.WithLabelValues(name, ep)
	t.numShardsMetric = t.metrics.numShards.WithLabelValues(name, ep)
	t.failedSamplesTotal = t.metrics.failedSamplesTotal.WithLabelValues(name, ep)
	t.sentBatchDuration = t.metrics.sentBatchDuration.WithLabelValues(name, ep)
	t.succeededSamplesTotal = t.metrics.succeededSamplesTotal.WithLabelValues(name, ep)
	t.retriedSamplesTotal = t.metrics.retriedSamplesTotal.WithLabelValues(name, ep)
	t.shardCapacity = t.metrics.shardCapacity.WithLabelValues(name, ep)
	t.maxNumShards = t.metrics.maxNumShards.WithLabelValues(name, ep)
	t.minNumShards = t.metrics.minNumShards.WithLabelValues(name, ep)
	t.desiredNumShards = t.metrics.desiredNumShards.WithLabelValues(name, ep)
	t.bytesSent = t.metrics.bytesSent.WithLabelValues(name, ep)

	// Initialise some metrics.
	t.shardCapacity.Set(float64(t.cfg.Capacity))
	t.pendingSamplesMetric.Set(0)
	t.maxNumShards.Set(float64(t.cfg.MaxShards))
	t.minNumShards.Set(float64(t.cfg.MinShards))
	t.desiredNumShards.Set(float64(t.cfg.MinShards))

	// 1. 启动 t.numShards 个数据队列
	t.shards.start(t.numShards)



	// 2. 启动 wal 文件监听服务:
	//  服务启动定时任务，每 5s 读取一次 wal 文件的 copy 文件，而不是直接打开 wal 原始文件。
	//  读取文件时判断文件是否是被写入文件，若正在写入数据，还需要考虑写入和读取之间的平衡关系。
	//  wal 监听器获取到的数据通过关联的 writeTo 对象发送到数据队列中。
	//  每次发送的数据都只会发送到一个指定的 shard 中，shard 编号通过计算 sample 获得。
	t.watcher.Start()




	t.wg.Add(2)

	// 3. 定时计算所需的 shard 数目，触发 reshard 信号，以优化数据传送效率。
	go t.updateShardsLoop()

	// 4. 监听 reshard 信号，执行 reshard 。
	go t.reshardLoop()
}

// Stop stops sending samples to the remote storage and waits for pending
// sends to complete.
func (t *QueueManager) Stop() {
	level.Info(t.logger).Log("msg", "Stopping remote storage...")
	defer level.Info(t.logger).Log("msg", "Remote storage stopped.")

	close(t.quit)
	t.wg.Wait()



	// Wait for all QueueManager routines to end before stopping shards and WAL watcher.
	//
	// This is to ensure we don't end up executing a reshard and shards.stop() at the same time,
	// which causes a closed channel panic.
	t.shards.stop()
	t.watcher.Stop()



	// On shutdown, release the strings in the labels from the intern pool.
	t.seriesMtx.Lock()
	for _, labels := range t.seriesLabels {
		releaseLabels(labels)
	}
	t.seriesMtx.Unlock()




	// Delete metrics so we don't have alerts for queues that are gone.
	name := t.client.Name()
	ep := t.client.Endpoint()
	t.metrics.queueHighestSentTimestamp.DeleteLabelValues(name, ep)
	t.metrics.queuePendingSamples.DeleteLabelValues(name, ep)
	t.metrics.enqueueRetriesTotal.DeleteLabelValues(name, ep)
	t.metrics.droppedSamplesTotal.DeleteLabelValues(name, ep)
	t.metrics.numShards.DeleteLabelValues(name, ep)
	t.metrics.failedSamplesTotal.DeleteLabelValues(name, ep)
	t.metrics.sentBatchDuration.DeleteLabelValues(name, ep)
	t.metrics.succeededSamplesTotal.DeleteLabelValues(name, ep)
	t.metrics.retriedSamplesTotal.DeleteLabelValues(name, ep)
	t.metrics.shardCapacity.DeleteLabelValues(name, ep)
	t.metrics.maxNumShards.DeleteLabelValues(name, ep)
	t.metrics.minNumShards.DeleteLabelValues(name, ep)
	t.metrics.desiredNumShards.DeleteLabelValues(name, ep)
}



// StoreSeries keeps track of which series we know about for lookups when sending samples to remote.
//
//
//
func (t *QueueManager) StoreSeries(series []record.RefSeries, index int) {

	t.seriesMtx.Lock()
	defer t.seriesMtx.Unlock()


	for _, s := range series {

		// 把 t.externalLabels 合并到 s.Labels 中，遇到同名标签则取 s.Labels 中的标签值，返回合并后的标签集。
		ls := processExternalLabels(s.Labels, t.externalLabels)

		// relabel
		lbls := relabel.Process(ls, t.relabelConfigs...)

		// 如果 relabel 之后无标签，在丢弃当前数据，continue
		if len(lbls) == 0 {
			t.droppedSeries[s.Ref] = struct{}{}
			continue
		}

		//
		t.seriesSegmentIndexes[s.Ref] = index



		internLabels(lbls)



		// We should not ever be replacing a series labels in the map,
		//
		// but just in case we do we need to ensure we do not leak the replaced interned strings.
		if orig, ok := t.seriesLabels[s.Ref]; ok {
			releaseLabels(orig)
		}


		t.seriesLabels[s.Ref] = lbls
	}
}









// SeriesReset is used when reading a checkpoint.
// SeriesReset 用于读取检查点。
//
// WAL Watcher should have stored series records with the checkpoints index number,
// so we can now delete any ref ID's lower than that # from the two maps.
//
// WAL Watcher 应该存储带有检查点索引号的时序数据，
//

func (t *QueueManager) SeriesReset(index int) {
	t.seriesMtx.Lock()
	defer t.seriesMtx.Unlock()

	// Check for series that are in segments older than the checkpoint that were not also present in the checkpoint.
	//


	for k, v := range t.seriesSegmentIndexes {
		if v < index {
			delete(t.seriesSegmentIndexes, k)
			releaseLabels(t.seriesLabels[k])
			delete(t.seriesLabels, k)
			delete(t.droppedSeries, k)
		}
	}
}






func internLabels(lbls labels.Labels) {
	for i, l := range lbls {
		lbls[i].Name = interner.intern(l.Name)
		lbls[i].Value = interner.intern(l.Value)
	}
}

func releaseLabels(ls labels.Labels) {
	for _, l := range ls {
		interner.release(l.Name)
		interner.release(l.Value)
	}
}




// processExternalLabels merges externalLabels into ls.
// If ls contains a label in externalLabels, the value in ls wins.
//
// 把 externalLabels 合并到 ls 中。如果某个标签既在 ls 中，又在 externalLabels 中，则选取 ls 中的值 。
//
func processExternalLabels(ls labels.Labels, externalLabels labels.Labels) labels.Labels {
	i, j, result := 0, 0, make(labels.Labels, 0, len(ls)+len(externalLabels))
	for i < len(ls) && j < len(externalLabels) {


		if ls[i].Name < externalLabels[j].Name {
			result = append(result, labels.Label{
				Name:  ls[i].Name,
				Value: ls[i].Value,
			})
			i++
		} else if ls[i].Name > externalLabels[j].Name {
			result = append(result, externalLabels[j])
			j++
		} else {
			result = append(result, labels.Label{
				Name:  ls[i].Name,
				Value: ls[i].Value,
			})
			i++
			j++
		}
	}
	for ; i < len(ls); i++ {
		result = append(result, labels.Label{
			Name:  ls[i].Name,
			Value: ls[i].Value,
		})
	}
	result = append(result, externalLabels[j:]...)
	return result
}

func (t *QueueManager) updateShardsLoop() {
	defer t.wg.Done()

	// 创建定时器（10秒）
	ticker := time.NewTicker(shardUpdateDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			// 计算所需的 shard 数目
			desiredShards := t.calculateDesiredShards()
			if desiredShards == t.numShards {
				continue
			}

			// Resharding can take some time, and we want this loop to stay close to shardUpdateDuration.
			// Resharding 可能要花费一些时间，我们希望开销和 shardUpdateDuration 差不多。
			select {

			// 把 desiredShards 写入管道中，reshardLoop() 协程会监听该管道，执行 resharding 。
			case t.reshardChan <- desiredShards:
				level.Info(t.logger).Log("msg", "Remote storage resharding", "from", t.numShards, "to", desiredShards)
				t.numShards = desiredShards
			default:
				level.Info(t.logger).Log("msg", "Currently resharding, skipping.")
			}
		case <-t.quit:
			return
		}
	}
}


// calculateDesiredShards returns the number of desired shards, which will be
// the current QueueManager.numShards if resharding should not occur for reasons
// outlined in this functions implementation. It is up to the caller to reshard, or not,
// based on the return value.
//

func (t *QueueManager) calculateDesiredShards() int {
	t.samplesOut.tick()
	t.samplesDropped.tick()
	t.samplesOutDuration.tick()

	// We use the number of incoming samples as a prediction of how much work we
	// will need to do next iteration.  We add to this any pending samples
	// (received - send) so we can catch up with any backlog. We use the average
	// outgoing batch latency to work out how many shards we need.
	var (
		samplesInRate      = t.samplesIn.rate()
		samplesOutRate     = t.samplesOut.rate()
		samplesKeptRatio   = samplesOutRate / (t.samplesDropped.rate() + samplesOutRate)
		samplesOutDuration = t.samplesOutDuration.rate() / float64(time.Second)
		samplesPendingRate = samplesInRate*samplesKeptRatio - samplesOutRate
		highestSent        = t.highestSentTimestampMetric.Get()
		highestRecv        = highestTimestamp.Get()
		delay              = highestRecv - highestSent
		samplesPending     = delay * samplesInRate * samplesKeptRatio
	)


	if samplesOutRate <= 0 {
		return t.numShards
	}


	// We shouldn't reshard if Prometheus hasn't been able to send to the
	// remote endpoint successfully within some period of time.

	minSendTimestamp := time.Now().Add(-2 * time.Duration(t.cfg.BatchSendDeadline)).Unix()
	lsts := atomic.LoadInt64(&t.lastSendTimestamp)
	if lsts < minSendTimestamp {
		level.Warn(t.logger).Log("msg", "Skipping resharding, last successful send was beyond threshold", "lastSendTimestamp", lsts, "minSendTimestamp", minSendTimestamp)
		return t.numShards
	}

	// When behind we will try to catch up on a proporation of samples per tick.
	// This works similarly to an integral accumulator in that pending samples
	// is the result of the error integral.
	const integralGain = 0.1 / float64(shardUpdateDuration/time.Second)

	var (
		timePerSample = samplesOutDuration / samplesOutRate
		desiredShards = timePerSample * (samplesInRate*samplesKeptRatio + integralGain*samplesPending)
	)
	t.desiredNumShards.Set(desiredShards)
	level.Debug(t.logger).Log("msg", "QueueManager.calculateDesiredShards",
		"samplesInRate", samplesInRate,
		"samplesOutRate", samplesOutRate,
		"samplesKeptRatio", samplesKeptRatio,
		"samplesPendingRate", samplesPendingRate,
		"samplesPending", samplesPending,
		"samplesOutDuration", samplesOutDuration,
		"timePerSample", timePerSample,
		"desiredShards", desiredShards,
		"highestSent", highestSent,
		"highestRecv", highestRecv,
	)

	// Changes in the number of shards must be greater than shardToleranceFraction.
	var (
		lowerBound = float64(t.numShards) * (1. - shardToleranceFraction)
		upperBound = float64(t.numShards) * (1. + shardToleranceFraction)
	)
	level.Debug(t.logger).Log("msg", "QueueManager.updateShardsLoop",
		"lowerBound", lowerBound, "desiredShards", desiredShards, "upperBound", upperBound)
	if lowerBound <= desiredShards && desiredShards <= upperBound {
		return t.numShards
	}

	numShards := int(math.Ceil(desiredShards))

	// Do not downshard if we are more than ten seconds back.
	if numShards < t.numShards && delay > 10.0 {
		level.Debug(t.logger).Log("msg", "Not downsharding due to being too far behind")
		return t.numShards
	}


	if numShards > t.cfg.MaxShards {
		numShards = t.cfg.MaxShards
	} else if numShards < t.cfg.MinShards {
		numShards = t.cfg.MinShards
	}
	return numShards
}

func (t *QueueManager) reshardLoop() {
	defer t.wg.Done()

	for {
		select {
		case numShards := <-t.reshardChan:

			// We start the newShards after we have stopped (the therefore completely flushed) the oldShards,
			// to guarantee we only every deliver samples in order.

			// reshard:
			// 	1. 先停止所有旧队列
			t.shards.stop()
			// 	2. 再启动 numShards 个新队列
			t.shards.start(numShards)
		case <-t.quit:
			return
		}
	}
}


func (t *QueueManager) newShards() *shards {
	s := &shards{
		qm:   t,
		done: make(chan struct{}),
	}
	return s
}


// 样本结构体
type sample struct {
	labels labels.Labels	// 标签集
	t      int64			// 时间戳
	v      float64			// 值
}


//
type shards struct {

	// With the WAL, this is never actually contended.
	mtx sync.RWMutex

	// 当前 shards 归属的 manager
	qm  *QueueManager

	// 采样队列数组，每个采样队列被一个协程监听、处理
	queues []chan sample

	// Emulate a wait group with a channel and an atomic int, as you cannot select on a wait group.
	// 使用 channel 和原子变量来模拟 WaitGroup ，因为不能将 WaitGroup 放到 select 中。
	done chan struct{}

	// 当前正在运行的采样协程
	running int32

	// Soft shutdown context will prevent new enqueues and deadlocks.
	// 软关闭
	softShutdown chan struct{}

	// Hard shutdown context is used to terminate outgoing HTTP connections after giving them a chance to terminate.
	// 硬关闭
	hardShutdown context.CancelFunc
}




// start the shards; must be called before any call to enqueue.
//
// 启动 n 个数据队列，每条队列都会执行数据的批量发送操作。
//
// 目前有两种批量发送策略，定量发送和定时发送
//	 MaxSamplesPerSend 用于设置来每次发送最大数量，若队列中数据大于最大值，则执行批量发送操作；
//	 BatchSendDeadline 用于设置最大等待时间，若队列等待时间超过最大时间，无论队列中有多少数据都会发送。
//
// 发送失败会进行无限重试，每次重试都等待时间都会增加，直到达到最大值 MaxBackoff 。
//
func (s *shards) start(n int) {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// 创建 n 个采样队列 newQueues[0...]
	newQueues := make([]chan sample, n)
	for i := 0; i < n; i++ {
		newQueues[i] = make(chan sample, s.qm.cfg.Capacity)
	}

	// 初始化 s 的成员变量
	s.queues = newQueues
	var hardShutdownCtx context.Context
	hardShutdownCtx, s.hardShutdown = context.WithCancel(context.Background())
	s.softShutdown = make(chan struct{})
	s.running = int32(n)
	s.done = make(chan struct{})


	// 创建 n 个 runShard() 协程，每个负责从采样队列 newQueues[i] 中读取样本数据、缓存到数组、批量的发送到远程存储。
	for i := 0; i < n; i++ {
		go s.runShard(hardShutdownCtx, i, newQueues[i])
	}

	// 上报协程数
	s.qm.numShardsMetric.Set(float64(n))
}



// stop the shards;
// subsequent call to enqueue will return false.
//
// 关闭 shards ，所有后续的样本入队请求都会失败并返回 false 。
//
func (s *shards) stop() {

	// Attempt a clean shutdown, but only wait flushDeadline for all the shards to cleanly exit.
	//
	// As we're doing RPCs, enqueue can block indefinitely.
	//
	// We must be able so call stop concurrently, hence we can only take the RLock here.

	s.mtx.RLock()
	close(s.softShutdown)
	s.mtx.RUnlock()

	// Enqueue should now be unblocked, so we can take the write lock.
	//
	// This also ensures we don't race with writes to the queues, and get a panic: send on closed channel.
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// 关闭 n 个采样队列，触发对应的 n 个 runShard() 后台协程退出
	for _, queue := range s.queues {
		close(queue)
	}

	select {
	// 当所有的 runShard() 协程都退出后，s.done 信号管道会被关闭，此时可以安全的退出。
	case <-s.done:
		return
	// 到达一定超时时间后，所有的协程未完成退出，则打印错误信息，后面进行强制关闭。
	case <-time.After(s.qm.flushDeadline):
		level.Error(s.qm.logger).Log("msg", "Failed to flush all samples on shutdown")
	}

	// Force an unclean shutdown.

	// 硬关闭
	s.hardShutdown()

	// ???
	<-s.done
}



// enqueue a sample.
// 样本入队。
//
// If we are currently in the process of shutting down or resharding, will return false;
// 如果我们目前正在关闭中，将返回 false ；
//
// in this case, you should back off and retry.
// 在这种情况下，您应该进行重试。
func (s *shards) enqueue(ref uint64, sample sample) bool {

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	select {
	case <-s.softShutdown:
		return false
	default:
	}

	// 根据 ref 确定 shard 编号
	shard := uint64(ref) % uint64(len(s.queues))
	select {
	case <-s.softShutdown:
		return false
	//样本入队，等待被后台协程采集和处理（写远程存储）
	case s.queues[shard] <- sample:
		return true
	}
}

// 不断地从 queue 中读取样本数据、缓存到数组，然后批量的发送到远程存储。
func (s *shards) runShard(ctx context.Context, shardID int, queue chan sample) {


	// 当所有的协程都退出后，s.running 为 0 ， 此时关闭 s.done 信号管道，通知 stop() 函数。
	defer func() {
		if atomic.AddInt32(&s.running, -1) == 0 {
			close(s.done)
		}
	}()

	// Send batches of at most MaxSamplesPerSend samples to the remote storage.
	// If we have fewer samples than that, flush them out after a deadline anyways.
	//
	// 将最多为 MaxSamplesPerSend 的样本批量发送到远程存储。
	// 如果我们的样品比 MaxSamplesPerSend 少，则在超时到期后发送。

	var (
		max            = s.qm.cfg.MaxSamplesPerSend
		nPending       = 0
		pendingSamples = allocateTimeSeries(max)
		buf            []byte
	)


	// 定时器
	timer := time.NewTimer(time.Duration(s.qm.cfg.BatchSendDeadline))

	// 如何安全的关闭 timer ???
	stop := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	defer stop()



	for {
		select {
		case <-ctx.Done():
			return

		// 从 queue 中不断读取样本数据
		case sample, ok := <-queue:

			// 如果队列被关闭，而还有缓存的样本尚未发送，则将它们 flush 到 remote storage 。
			if !ok {
				if nPending > 0 {
					level.Debug(s.qm.logger).Log("msg", "Flushing samples to remote storage...", "count", nPending)
					s.sendSamples(ctx, pendingSamples[:nPending], &buf)
					s.qm.pendingSamplesMetric.Sub(float64(nPending))
					level.Debug(s.qm.logger).Log("msg", "Done flushing.")
				}
				return
			}



			// Number of pending samples is limited by the fact that sendSamples (via sendSamplesWithBackoff)
			// retries endlessly, so once we reach max samples, if we can never send to the endpoint we'll
			// stop reading from the queue.
			//
			// This makes it safe to reference pendingSamples by index.



			// 将 sample 样本存储到数组 pendingSamples 中暂存
			pendingSamples[nPending].Labels = labelsToLabelsProto(sample.labels, pendingSamples[nPending].Labels)
			pendingSamples[nPending].Samples[0].Timestamp = sample.t
			pendingSamples[nPending].Samples[0].Value = sample.v

			// 增加缓存样本数（数组下标）
			nPending++

			// 上报
			s.qm.pendingSamplesMetric.Inc()


			// 如果缓存的样本数目超过 max 则发送它们到 remote storage，然后重置 nPending 为 0，并重置计数器 。
			if nPending >= max {
				s.sendSamples(ctx, pendingSamples, &buf)
				nPending = 0
				s.qm.pendingSamplesMetric.Sub(float64(max))

				stop()
				timer.Reset(time.Duration(s.qm.cfg.BatchSendDeadline))
			}

		// 定时间隔
		case <-timer.C:

			// 如果 nPending 的样本数不为 0 ，则发送它们到 remote storage ，然后重置 nPending 为 0 。
			if nPending > 0 {
				level.Debug(s.qm.logger).Log("msg", "runShard timer ticked, sending samples", "samples", nPending, "shard", strconv.Itoa(shardID))
				s.sendSamples(ctx, pendingSamples[:nPending], &buf)
				s.qm.pendingSamplesMetric.Sub(float64(nPending))
				nPending = 0
			}

			// 重置定时器，以便重新定时
			timer.Reset(time.Duration(s.qm.cfg.BatchSendDeadline))
		}
	}
}

func (s *shards) sendSamples(ctx context.Context, samples []prompb.TimeSeries, buf *[]byte) {
	begin := time.Now()

	// 将样本 samples 发送到远程存储
	err := s.sendSamplesWithBackoff(ctx, samples, buf)

	// 出错则打印日志和上报
	if err != nil {
		level.Error(s.qm.logger).Log("msg", "non-recoverable error", "count", len(samples), "err", err)
		s.qm.failedSamplesTotal.Add(float64(len(samples)))
	}


	// These counters are used to calculate the dynamic sharding,
	// and as such should be maintained irrespective of success or failure.

	// 上报样本数、发送延迟
	s.qm.samplesOut.incr(int64(len(samples)))
	s.qm.samplesOutDuration.incr(int64(time.Since(begin)))
}


// sendSamples to the remote storage with backoff for recoverable errors.
//
// sendSamples() 将样本发送到远程存储，遇到可重试的错误，会自动重试。
func (s *shards) sendSamplesWithBackoff(ctx context.Context, samples []prompb.TimeSeries, buf *[]byte) error {

	backoff := s.qm.cfg.MinBackoff

	// 构造写请求
	req, highest, err := buildWriteRequest(samples, *buf)
	*buf = req
	if err != nil {
		// Failing to build the write request is non-recoverable, since it will
		// only error if marshaling the proto to bytes fails.
		return err
	}

	// 带重试的发送请求
	for {

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		begin := time.Now()

		// 调用 Store() 将样本存储在远程存储器中。
		err := s.qm.client.Store(ctx, req)

		// 上报延迟
		s.qm.sentBatchDuration.Observe(time.Since(begin).Seconds())

		// 发送成功，上报其它数据
		if err == nil {
			s.qm.succeededSamplesTotal.Add(float64(len(samples)))
			s.qm.bytesSent.Add(float64(len(req)))
			s.qm.highestSentTimestampMetric.Set(float64(highest / 1000))
			atomic.StoreInt64(&s.qm.lastSendTimestamp, time.Now().Unix())
			return nil
		}

		// 发送失败
		if _, ok := err.(recoverableError); !ok {
			return err
		}

		// 上报错误信息、打印日志
		s.qm.retriedSamplesTotal.Add(float64(len(samples)))
		level.Debug(s.qm.logger).Log("msg", "Failed to send batch, retrying", "err", err)

		// 等待、重试
		time.Sleep(time.Duration(backoff))
		backoff = backoff * 2
		if backoff > s.qm.cfg.MaxBackoff {
			backoff = s.qm.cfg.MaxBackoff
		}
	}
}

func buildWriteRequest(samples []prompb.TimeSeries, buf []byte) ([]byte, int64, error) {

	var highest int64

	// 遍历 samples ，找出最大（最近）时间戳
	for _, ts := range samples {
		// At the moment we only ever append a TimeSeries with a single sample in it.
		if ts.Samples[0].Timestamp > highest {
			highest = ts.Samples[0].Timestamp
		}
	}

	// 构造写请求
	req := &prompb.WriteRequest{
		Timeseries: samples,
	}

	// 序列化
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, highest, err
	}

	// snappy uses len() to see if it needs to allocate a new slice.
	// Make the buffer as long as possible.
	//
	// 重置 buf
	if buf != nil {
		buf = buf[0:cap(buf)]
	}

	// 数据压缩
	compressed := snappy.Encode(buf, data)

	// 返回
	return compressed, highest, nil
}


func allocateTimeSeries(capacity int) []prompb.TimeSeries {

	timeseries := make([]prompb.TimeSeries, capacity)


	// We only ever send one sample per timeseries, so preallocate with length one.
	for i := range timeseries {
		timeseries[i].Samples = []prompb.Sample{{}}
	}

	return timeseries
}
