// Copyright 2020 The Prometheus Authors
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

// This file holds boilerplate adapters for generic MergeSeriesSet and MergeQuerier functions, so we can have one optimized
// solution that works for both ChunkSeriesSet as well as SeriesSet.

package storage

import "github.com/blastbao/prometheus/pkg/labels"





// 通用查询器接口
type genericQuerier interface {

	// 基础查询器接口
	baseQuerier

	// 查询函数：根据给定的搜索参数查询匹配的时序数据
	Select(bool, *SelectHints, ...*labels.Matcher) (genericSeriesSet, Warnings, error)
}


// 通用时序数据集合
type genericSeriesSet interface {
	Next() bool
	At() Labels
	Err() error
}



type genericSeriesMergeFunc func(...Labels) Labels

type genericSeriesSetAdapter struct {
	SeriesSet
}

func (a *genericSeriesSetAdapter) At() Labels {
	return a.SeriesSet.At()
}

type genericChunkSeriesSetAdapter struct {
	ChunkSeriesSet
}

func (a *genericChunkSeriesSetAdapter) At() Labels {
	return a.ChunkSeriesSet.At()
}

type genericQuerierAdapter struct {
	baseQuerier

	// One-of. If both are set, Querier will be used.
	q  Querier
	cq ChunkQuerier
}

func (q *genericQuerierAdapter) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (genericSeriesSet, Warnings, error) {
	if q.q != nil {
		s, w, err := q.q.Select(sortSeries, hints, matchers...)
		return &genericSeriesSetAdapter{s}, w, err
	}
	s, w, err := q.cq.Select(sortSeries, hints, matchers...)
	return &genericChunkSeriesSetAdapter{s}, w, err
}

func newGenericQuerierFrom(q Querier) genericQuerier {
	return &genericQuerierAdapter{baseQuerier: q, q: q}
}

func newGenericQuerierFromChunk(cq ChunkQuerier) genericQuerier {
	return &genericQuerierAdapter{baseQuerier: cq, cq: cq}
}



// 查询器适配器
type querierAdapter struct {
	// 通用查询器接口
	genericQuerier
}

// 查询函数
func (q *querierAdapter) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (SeriesSet, Warnings, error) {
	s, w, err := q.genericQuerier.Select(sortSeries, hints, matchers...)
	return &seriesSetAdapter{s}, w, err
}


// 时序数据集适配器
type seriesSetAdapter struct {
	// 通用时序数据集合
	genericSeriesSet
}

// 获取当前时序数据
func (a *seriesSetAdapter) At() Series {
	return a.genericSeriesSet.At().(Series)
}


type chunkQuerierAdapter struct {
	genericQuerier
}

func (q *chunkQuerierAdapter) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (ChunkSeriesSet, Warnings, error) {
	s, w, err := q.genericQuerier.Select(sortSeries, hints, matchers...)
	return &chunkSeriesSetAdapter{s}, w, err
}

type chunkSeriesSetAdapter struct {
	genericSeriesSet
}

func (a *chunkSeriesSetAdapter) At() ChunkSeries {
	return a.genericSeriesSet.At().(ChunkSeries)
}


type seriesMergerAdapter struct {

	// VerticalSeriesMergeFunc 将具有相同标签的序列合并在一起后返回，它还必须处理时间重叠的序列。
	VerticalSeriesMergeFunc

	// 时序数据集数组
	buf []Series
}


func (a *seriesMergerAdapter) Merge(s ...Labels) Labels {
	// 清空 a.buf
	a.buf = a.buf[:0]

	// 把时序数据添加到 a.buf 中
	for _, ser := range s {
		a.buf = append(a.buf, ser.(Series))
	}

	// 调用 a.VerticalSeriesMergeFunc() 把 a.buf 中的所有时序数据进行合并
	return a.VerticalSeriesMergeFunc(a.buf...)
}



type chunkSeriesMergerAdapter struct {
	VerticalChunkSeriesMergerFunc
	buf []ChunkSeries
}

func (a *chunkSeriesMergerAdapter) Merge(s ...Labels) Labels {
	a.buf = a.buf[:0]
	for _, ser := range s {
		a.buf = append(a.buf, ser.(ChunkSeries))
	}
	return a.VerticalChunkSeriesMergerFunc(a.buf...)
}
