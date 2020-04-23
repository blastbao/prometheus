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
	"sync"
	"sync/atomic"
	"time"
)





// ewmaRate tracks an exponentially weighted moving average of a per-second rate.

type ewmaRate struct {

	// Keep all 64bit atomically accessed variables at the top of this struct.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG for more info.

	newEvents int64

	alpha    float64
	interval time.Duration
	lastRate float64
	init     bool
	mutex    sync.Mutex
}


// newEWMARate always allocates a new ewmaRate,
// as this guarantees the atomically accessed int64 will be aligned on ARM.
//
// See prometheus#2666.
func newEWMARate(alpha float64, interval time.Duration) *ewmaRate {
	return &ewmaRate{
		alpha:    alpha,	// 权重
		interval: interval,	// 定时间隔
	}
}

// rate returns the per-second rate.
func (r *ewmaRate) rate() float64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.lastRate
}

// tick assumes to be called every r.interval.
//
// 每隔 r.interval 秒会调用一次 tick()，用来统计这段时间内 "平均每秒通过 r.incr() 增加的数量"，
func (r *ewmaRate) tick() {

	// stores 0 into &r.newEvents and returns the previous r.newEvents value.
	newEvents := atomic.SwapInt64(&r.newEvents, 0)

	// 计算每秒的事件数增量
	instantRate := float64(newEvents) / r.interval.Seconds()

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.init {
		// 非首次运行，计算相邻两次每秒增量的差值 * 权重
		r.lastRate += r.alpha * (instantRate - r.lastRate)
	} else if newEvents > 0 {
		// 首次运行，初始化成员
		r.init = true
		r.lastRate = instantRate
	}
}

// inc counts one event.
func (r *ewmaRate) incr(incr int64) {
	atomic.AddInt64(&r.newEvents, incr)
}
