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

package storage

import (
	"math"

	"github.com/blastbao/prometheus/tsdb/chunkenc"
)



// BufferedSeriesIterator wraps an iterator with a look-back buffer.
//
// BufferedSeriesIterator 把支持 look-back 的 buffer 和迭代器封装到一起。
//
type BufferedSeriesIterator struct {
	it    chunkenc.Iterator	// 迭代器
	buf   *sampleRing		// 环形缓冲，保存着 [t - delta, t] 时间区间中的样本
	delta int64				// 控制时间区间大小

	lastTime int64
	ok       bool
}

// NewBuffer returns a new iterator that buffers the values within the time range
// of the current element and the duration of delta before, initialized with an
// empty iterator. Use Reset() to set an actual iterator to be buffered.
//
//
func NewBuffer(delta int64) *BufferedSeriesIterator {
	return NewBufferIterator(chunkenc.NewNopIterator(), delta)
}


// NewBufferIterator returns a new iterator that buffers the values within the
// time range of the current element and the duration of delta before.
//
func NewBufferIterator(it chunkenc.Iterator, delta int64) *BufferedSeriesIterator {

	bit := &BufferedSeriesIterator{
		buf:   newSampleRing(delta, 16), 	// 环形缓冲大小默认为 16，内部会自动扩容（2倍扩容）
		delta: delta,							// 默认 5m
	}

	bit.Reset(it)

	return bit
}

// Reset re-uses the buffer with a new iterator,
// resetting the buffered time delta to its original value.
//
//
func (b *BufferedSeriesIterator) Reset(it chunkenc.Iterator) {
	b.it = it
	b.lastTime = math.MinInt64 	// 9983int64 最小值
	b.ok = true					// it 中是否有数据可读
	b.buf.reset()
	b.buf.delta = b.delta
	it.Next()
}

// ReduceDelta lowers the buffered time delta, for the current SeriesIterator only.
func (b *BufferedSeriesIterator) ReduceDelta(delta int64) bool {
	return b.buf.reduceDelta(delta)
}

// PeekBack returns the nth previous element of the iterator.
//
// If there is none buffered, ok is false.
func (b *BufferedSeriesIterator) PeekBack(n int) (t int64, v float64, ok bool) {
	return b.buf.nthLast(n)
}

// Buffer returns an iterator over the buffered data.
// Invalidates previously returned iterators.
func (b *BufferedSeriesIterator) Buffer() chunkenc.Iterator {
	return b.buf.iterator()
}


// Seek advances the iterator to the element at time t or greater.
//
// Seek() 把迭代器的指针指向首个时间戳大于等于 t 的元素。
func (b *BufferedSeriesIterator) Seek(t int64) bool {

	// 确定时间区间下界
	t0 := t - b.buf.delta

	// If the delta would cause us to seek backwards,
	// preserve the buffer and just continue regular advancement while filling the buffer on the way.

	// 若当前 buf 中最新元素的时间戳都小于 t0 ，则重置(清空) b.buf ，然后调用 b.it.Seek(t0) 让 b.it 移进到 t0 ，
	// 这样，后面通过 for 循环不断 b.it.Next() 往 buf 中填入 [t0, t] 区间的样本。
	if b.lastTime < t0 {
		b.buf.reset()
		b.ok = b.it.Seek(t0)
		if !b.ok {
			return false
		}
		b.lastTime, _ = b.Values()
	}

	// (1) 若当前 buf 中最新元素的时间戳大于等于 t0 且大于等于 t ，则 b.buf 中已经缓存了 [t0, t] 区间的样本，直接返回 true 。
	// (2) 若当前 buf 中最新元素的时间戳大于等于 t0 但小于 t ，则 b.buf 中仅缓存了 [t0, t] 区间的部分样本，需要继续填充。
	// (3) 若当前 buf 中最新元素的时间戳小于 t0 ，则 b.buf 中不包含 [t0, t] 区间的任何样本，前面的 if 分支会重置 b.buf 和 b.it，
	// 	   并通过 b.it.Seek(t0) 返回首个 >= t0 的样本，然后判断：
	// 		(3.1) 若当前样本 >= t ，则满足要求，直接返回。
	// 		(3.2) 若当前样本 < t ，则不断 b.Next() 从 it 读取样本并存入 b.buf 中，直到遇到首个满足 >= t 的样本为止。

	if b.lastTime >= t {
		return true
	}

	//
	for b.Next() {
		if b.lastTime >= t {
			return true
		}
	}

	// 若 b.it 中无数据可读，返回 false 。
	return false
}

// Next advances the iterator to the next element.
func (b *BufferedSeriesIterator) Next() bool {

	// 若 ok 为 false ，则 it 已无数据可读，返回 false
	if !b.ok {
		return false
	}

	// 从 it 中取当前元素，塞入 buf 中。
	b.buf.add(b.it.At())

	// 从 it 读取下个元素
	b.ok = b.it.Next()

	// 若 ok 为 true，则更新 b.lastTime 为当前元素 timestamp
	if b.ok {
		b.lastTime, _ = b.Values()
	}

	// 若 it 中还有数据可读，返回 true ，否则 false 。
	return b.ok
}

// Values returns the current element of the iterator.
func (b *BufferedSeriesIterator) Values() (int64, float64) {
	return b.it.At()
}

// Err returns the last encountered error.
func (b *BufferedSeriesIterator) Err() error {
	return b.it.Err()
}

// 样本
type sample struct {
	t int64		// 时间戳，ms
	v float64	// 值
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) V() float64 {
	return s.v
}


// sampleRing 环形缓冲中保留了 [t - delta, t] 区间中的样本，每当新添加样本或者调整 delta 大小时，需要移除过期的样本。
//
//
type sampleRing struct {

	delta int64

	buf   []sample // lookback buffer
	tail  int      // position of most recent element in ring buffer
	head  int      // position of first element in ring buffer
	count int      // number of elements in buffer

	it sampleRingIterator
}

func newSampleRing(delta int64, sz int) *sampleRing {

	r := &sampleRing{
		delta: delta,
		buf: make([]sample, sz),
	}
	r.reset()

	return r
}

func (r *sampleRing) reset() {
	r.count = 0
	r.tail = -1
	r.head = 0
}

func (r *sampleRing) at(i int) (int64, float64) {
	j := (r.head + i) % len(r.buf)
	s := r.buf[j]
	return s.t, s.v
}

// add adds a sample to the ring buffer and frees all samples that fall out of the delta range.
func (r *sampleRing) add(t int64, v float64) {

	// 当前缓冲区大小
	l := len(r.buf)

	// Grow the ring buffer if it fits no more elements.
	//
	// 数组已满，此时 (r.tail + 1) % len(r.buf) == r.head，不能容纳新元素，需要扩容。
	if l == r.count {
		// 扩容一倍
		buf := make([]sample, 2*l)
		// 把从 head 开始的共 l 个元素，拷贝到 l + head 处，分两步完成
		// （1）把 r.buf[r.head:] 拷贝到 buf[l+r.head:]
		copy(buf[l+r.head:], r.buf[r.head:])
		// （2）把 r.buf[:r.head] 拷贝到 buf[:r.head]
		copy(buf, r.buf[:r.head])

		// 更新相关变量
		r.buf = buf
		r.tail = r.head  // 这里相当于执行 (r.tail + 1) % len(r.buf)
		r.head += l
		l = 2 * l
	} else {
		// 数组未满，直接移动 r.tail + 1
		r.tail++
		// 相当于 r.tail % len(r.buf)，指针回绕
		if r.tail >= l {
			r.tail -= l
		}
	}

	// 把新元素插入到 r.tail 位置
	r.buf[r.tail] = sample{t: t, v: v}
	// 更新当前缓存的元素数
	r.count++

	// Free head of the buffer of samples that just fell out of the range.
	//
	// 环形缓冲中只保留 [t - r.delta, t] 区间中的样本，因此每当新添加样本，需要移除过期的样本。
	tmin := t - r.delta
	for r.buf[r.head].t < tmin {
		// 移除 r.head 的元素，分两步完成
		// (1) 移动头指针，(r.head + 1) % len(r.buf)
		r.head++
		if r.head >= l {
			r.head -= l
		}
		// (2) 更新当前缓存的元素数
		r.count--
	}
}

// reduceDelta lowers the buffered time delta,
// dropping any samples that are out of the new delta range.
func (r *sampleRing) reduceDelta(delta int64) bool {

	// 只允许往低调整
	if delta > r.delta {
		return false
	}

	// 更新
	r.delta = delta

	// 没有元素，则直接返回
	if r.count == 0 {
		return true
	}

	// Free head of the buffer of samples that just fell out of the range.
	//
	// 环形缓冲中只保留 [t - r.delta, t] 区间中的样本，当调低 r.delta 时，需要移除过期的样本。
	l := len(r.buf)
	tmin := r.buf[r.tail].t - delta
	for r.buf[r.head].t < tmin {
		// 移除 r.head 的元素，分两步完成
		// (1) 移动头指针，(r.head + 1) % len(r.buf)
		r.head++
		if r.head >= l {
			r.head -= l
		}
		// (2) 更新当前缓存的元素数
		r.count--
	}

	return true
}

// nthLast returns the nth most recent element added to the ring.
//
// nthLast 返回最近添加到 buf 中的第 n 个元素。
//
func (r *sampleRing) nthLast(n int) (int64, float64, bool) {
	if n > r.count {
		return 0, 0, false
	}
	t, v := r.at(r.count - n)
	return t, v, true
}


func (r *sampleRing) samples() []sample {

	// 构造切片 res
	res := make([]sample, r.count)

	// 把当前 buf 中元素拷贝到 res 中
	var k = r.head + r.count
	var j int
	if k > len(r.buf) {
		k = len(r.buf)
		j = r.count - k + r.head
	}

	// 分两步拷贝
	n := copy(res, r.buf[r.head:k])
	copy(res[n:], r.buf[:j])

	return res
}

// Returns the current iterator.
// Invalidates previously returned iterators.
func (r *sampleRing) iterator() chunkenc.Iterator {
	r.it.r = r
	r.it.i = -1
	return &r.it
}

type sampleRingIterator struct {
	r *sampleRing
	i int
}

func (it *sampleRingIterator) Next() bool {
	it.i++
	return it.i < it.r.count
}

func (it *sampleRingIterator) Seek(int64) bool {
	return false
}

func (it *sampleRingIterator) Err() error {
	return nil
}

func (it *sampleRingIterator) At() (int64, float64) {
	return it.r.at(it.i)
}
