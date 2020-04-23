// Copyright 2017 The Prometheus Authors
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
	"fmt"
	"sync"
	"time"

	"github.com/blastbao/prometheus/config"
	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/storage"
	"github.com/blastbao/prometheus/tsdb/wal"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	samplesIn = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "samples_in_total",
		Help:      "Samples in to remote storage, compare to samples out for queue managers.",
	})
	highestTimestamp = maxGauge{
		Gauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "highest_timestamp_in_seconds",
			Help:      "Highest timestamp that has come into the remote storage via the Appender interface, in seconds since epoch.",
		}),
	}
)

// WriteStorage represents all the remote write storage.
type WriteStorage struct {

	logger log.Logger
	mtx    sync.Mutex

	// metrics
	queueMetrics      *queueManagerMetrics
	watcherMetrics    *wal.WatcherMetrics
	liveReaderMetrics *wal.LiveReaderMetrics

	//
	configHash        string
	externalLabelHash string
	walDir            string


	//
	queues            map[string]*QueueManager

	samplesIn         *ewmaRate
	flushDeadline     time.Duration
}

// NewWriteStorage creates and runs a WriteStorage.
func NewWriteStorage(
	logger log.Logger,
	reg prometheus.Registerer,
	walDir string,
	flushDeadline time.Duration,
) *WriteStorage {


	if logger == nil {
		logger = log.NewNopLogger()
	}


	rws := &WriteStorage{
		// hash(RemoteWriteConfig-id) => *QueueManager
		queues:            make(map[string]*QueueManager),
		// metrics
		queueMetrics:      newQueueManagerMetrics(reg),
		watcherMetrics:    wal.NewWatcherMetrics(reg),
		liveReaderMetrics: wal.NewLiveReaderMetrics(reg),
		logger:            logger,
		flushDeadline:     flushDeadline,
		samplesIn:         newEWMARate(ewmaWeight, shardUpdateDuration),
		walDir:            walDir,
	}


	//
	go rws.run()


	return rws
}


// 每隔 10 秒会调用一次 rws.samplesIn.tick()，统计相邻两次的 "每秒写入的样本数" 的增量，来衡量数据量变化的稳定性。
func (rws *WriteStorage) run() {

	ticker := time.NewTicker(shardUpdateDuration)
	defer ticker.Stop()

	for range ticker.C {
		rws.samplesIn.tick()
	}

}


// ApplyConfig updates the state as the new config requires.
// Only stop & create queues which have changes.
func (rws *WriteStorage) ApplyConfig(conf *config.Config) error {


	rws.mtx.Lock()
	defer rws.mtx.Unlock()


	// 在应用配置 conf 之前，先检查 conf.RemoteWriteConfigs 和 conf.GlobalConfig.ExternalLabels 有无变更，若无变更，直接返回。
	configHash, err := toHash(conf.RemoteWriteConfigs)
	if err != nil {
		return err
	}

	externalLabelHash, err := toHash(conf.GlobalConfig.ExternalLabels)
	if err != nil {
		return err
	}

	// Remote write queues only need to change if the remote write config or external labels change.
	externalLabelUnchanged := externalLabelHash == rws.externalLabelHash
	if configHash == rws.configHash && externalLabelUnchanged {
		level.Debug(rws.logger).Log("msg", "Remote write config has not changed, no need to restart QueueManagers")
		return nil
	}

	rws.configHash = configHash
	rws.externalLabelHash = externalLabelHash

	newQueues := make(map[string]*QueueManager)
	newHashes := []string{}


	// 遍历远程存储的写配置
	for _, rwConf := range conf.RemoteWriteConfigs {


		// 计算配置项的 hash 作为唯一标识，每个远程写配置对应一个 *QueueManager
		hash, err := toHash(rwConf)
		if err != nil {
			return err
		}

		// Set the queue name to the config hash if the user has not set
		// a name in their remote write config so we can still differentiate
		// between queues that have the same remote write endpoint.
		//
		// 如果用户未在其远程写配置 rwConf 中设置名称，则设置其名称为 hash 值，这样即便远程 endpoint 相同仍能够通过 name 来区分。

		name := string(hash[:6])
		if rwConf.Name != "" {
			name = rwConf.Name
		}

		// Don't allow duplicate remote write configs.
		//
		// 如果 hash 值对应的远程写配置已经添加，则为重复配置，报错返回。
		if _, ok := newQueues[hash]; ok {
			return fmt.Errorf("duplicate remote write configs are not allowed, found duplicate for URL: %s", rwConf.URL)
		}

		// 检查 hash 值是否已经存在，若已经存在，检查 name 是否已经变化
		var nameUnchanged bool
		queue, ok := rws.queues[hash]
		if ok {
			nameUnchanged = queue.client.Name() == name
		}

		// 如果 hash 对应的配置没有变化，就复用旧的 QueueManager
		if externalLabelUnchanged && nameUnchanged {
			newQueues[hash] = queue
			delete(rws.queues, hash)
			continue
		}

		// 构造新的 http client 用来发送远程写请求
		c, err := NewClient(name, &ClientConfig{
			URL:              rwConf.URL,
			Timeout:          rwConf.RemoteTimeout,
			HTTPClientConfig: rwConf.HTTPClientConfig,
		})


		if err != nil {
			return err
		}

		// 构造新的 QueueManager 保存到 map[hash] 中，它负责 hash 对应的 RemoteWriteConf 的远程存储写的任务。
		newQueues[hash] = NewQueueManager(
			rws.queueMetrics,
			rws.watcherMetrics,
			rws.liveReaderMetrics,
			rws.logger,
			rws.walDir,
			rws.samplesIn,
			rwConf.QueueConfig,
			conf.GlobalConfig.ExternalLabels,
			rwConf.WriteRelabelConfigs,
			c,
			rws.flushDeadline,
		)

		// Keep track of which queues are new so we know which to start.
		newHashes = append(newHashes, hash)
	}


	// Anything remaining in rws.queues is a queue who's config has changed or was removed from the overall remote write config.
	//
	// rws.queues 中剩余的内容都是 已更改 或 已删除 的远程写配置，需要关闭它们。
	for _, q := range rws.queues {
		q.Stop()
	}

	// 新添加的远程写配置需要启动对应的 QueueManager，复用旧配置的则无需再次启动
	for _, hash := range newHashes {
		newQueues[hash].Start()
	}

	// 更新当前 rws 正在生效的 QueueManager 集合
	rws.queues = newQueues

	return nil
}

// Appender implements storage.Storage.
func (rws *WriteStorage) Appender() storage.Appender {
	return &timestampTracker{
		writeStorage: rws,
	}
}

// Close closes the WriteStorage.
func (rws *WriteStorage) Close() error {
	rws.mtx.Lock()
	defer rws.mtx.Unlock()
	for _, q := range rws.queues {
		q.Stop()
	}
	return nil
}

type timestampTracker struct {
	writeStorage     *WriteStorage
	samples          int64				// 写入样本数
	highestTimestamp int64				//
}

// Add implements storage.Appender.
func (t *timestampTracker) Add(_ labels.Labels, ts int64, _ float64) (uint64, error) {

	// 写入样本数 +1
	t.samples++

	// 如果当前样本的时间戳更大，则更新 t.highestTimestamp
	if ts > t.highestTimestamp {
		t.highestTimestamp = ts
	}

	return 0, nil
}

// AddFast implements storage.Appender.
func (t *timestampTracker) AddFast(_ uint64, ts int64, v float64) error {
	_, err := t.Add(nil, ts, v)
	return err
}

// Commit implements storage.Appender.
func (t *timestampTracker) Commit() error {

	// 在 commit 时，更新 samplesIn 计数
	t.writeStorage.samplesIn.incr(t.samples)

	// 上报 samplesIn 指标
	samplesIn.Add(float64(t.samples))

	// 上报 highestTimestamp 指标
	highestTimestamp.Set(float64(t.highestTimestamp / 1000)) // in second

	return nil
}


// Rollback implements storage.Appender.
func (*timestampTracker) Rollback() error {
	return nil
}
