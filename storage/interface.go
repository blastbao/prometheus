// Copyright 2014 The Prometheus Authors
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
	"context"
	"errors"

	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/tsdb/chunkenc"
	"github.com/blastbao/prometheus/tsdb/chunks"
	"github.com/blastbao/prometheus/tsdb/tombstones"
)

// The errors exposed.
var (
	ErrNotFound                    = errors.New("not found")
	ErrOutOfOrderSample            = errors.New("out of order sample")
	ErrDuplicateSampleForTimestamp = errors.New("duplicate sample for timestamp")
	ErrOutOfBounds                 = errors.New("out of bounds")
)

// Appendable allows creating appenders.
type Appendable interface {
	// Appender returns a new appender for the storage.
	Appender() Appender
}



// Storage ingests and manages samples, along with various indexes.
//
// All methods are goroutine-safe.
//
// Storage implements storage.SampleAppender.
// TODO(bwplotka): Add ChunkQueryable to Storage in next PR.
type Storage interface {

	// Queryable handles queries against a storage.
	Queryable

	// Appendable allows creating appenders.
	Appendable

	// StartTime returns the oldest timestamp stored in the storage.
	StartTime() (int64, error)

	// Close closes the storage and all its underlying resources.
	Close() error
}





// A Queryable handles queries against a storage.
//
//
// Use it when you need to have access to all samples without chunk encoding abstraction e.g promQL.
//
//
type Queryable interface {

	// Querier returns a new Querier on the storage.

	Querier(ctx context.Context, mint, maxt int64) (Querier, error)
}





// Querier provides querying access over time series data of a fixed time range.
type Querier interface {



	baseQuerier


	// Select returns a set of series that matches the given label matchers.
	//
	// Caller can specify if it requires returned series to be sorted.
	//
	//
	//
	// Prefer not requiring sorting for better performance.
	// It allows passing hints that can help in optimising select,
	// but it's up to implementation how this is used if used at all.



	// 根据给定的搜索参数（hints: 如起止时间、步进、聚合函数）获取匹配的时序数据，返回数据的样式就是 promQL 页面返回的格式。
	//
	//
	Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (SeriesSet, Warnings, error)


}






// A ChunkQueryable handles queries against a storage.
//
// Use it when you need to have access to samples in encoded format.
//
type ChunkQueryable interface {


	// ChunkQuerier returns a new ChunkQuerier on the storage.
	//
	//
	ChunkQuerier(ctx context.Context, mint, maxt int64) (ChunkQuerier, Warnings, error)
}





// ChunkQuerier provides querying access over time series data of a fixed time range.
//
// ChunkQuerier 提供了对一个确定时间范围内的时序数据进行查询的能力。
type ChunkQuerier interface {


	baseQuerier


	// Select returns a set of series that matches the given label matchers.
	//
	// Caller can specify if it requires returned series to be sorted.
	// Prefer not requiring sorting for better performance.
	//
	// It allows passing hints that can help in optimising select,
	// but it's up to implementation how this is used if used at all.
	//
	//
	// Select() 返回一组与给定标签匹配的 series 。
	// 调用者可以指定是否需要对返回的 series 进行排序，最好不排序以获得更好的性能。
	// Select() 允许传递有助于优化查询的提示（hints），但是否使用、如何使用则取决于具体实现。
	//
	// 参数说明:
	//	1. sortSeries: 是否对结果排序
	//	2. hints: 查询优化选项
	//  3. matchers: 标签匹配器
	//
	// 返回值说明:
	//	1. ChunkSeriesSet:
	//  2. Warnings:
	//	3. error:
	Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (ChunkSeriesSet, Warnings, error)
}



type baseQuerier interface {


	// LabelValues returns all potential values for a label name.
	// It is not safe to use the strings beyond the lifefime of the querier.
	//
	// LabelValues(name) 返回标签 name 的所有可能取值。
	LabelValues(name string) ([]string, Warnings, error)


	// LabelNames returns all the unique label names present in the block in sorted order.
	//
	// LabelNames() 按序返回存储 block 中存在的所有label名称。
	LabelNames() ([]string, Warnings, error)


	// Close releases the resources of the Querier.
	//
	// Close 释放 Querier 的资源。
	Close() error


}




// SelectHints specifies hints passed for data selections.
//
// This is used only as an option for implementation to use.
//
type SelectHints struct {

	// 时间区间
	Start int64 		// Start time in milliseconds for this select.
	End   int64 		// End time in milliseconds for this select.

	Step int64  		// Query step size in milliseconds.
	Func string 		// String representation of surrounding function or aggregation.

	// group by labels
	Grouping []string 	// List of label names used in aggregation.

	// without or by
	By       bool     	// Indicate whether it is without or by.


	Range    int64    	// Range vector selector range in milliseconds.
}




// QueryableFunc is an adapter to allow the use of ordinary functions as Queryables.
//
// It follows the idea of http.HandlerFunc.
type QueryableFunc func(ctx context.Context, mint, maxt int64) (Querier, error)




// Querier calls f() with the given parameters.
//
//
func (f QueryableFunc) Querier(ctx context.Context, mint, maxt int64) (Querier, error) {
	return f(ctx, mint, maxt)
}





// Appender provides batched appends against a storage.
// Appender 根据存储提供批量追加。
//
// It must be completed with a call to Commit or Rollback and must not be reused afterwards.
// 必须通过调用 Commit 或 Rollback 完成写入，在完成写入后不能重用。
//
// Operations on the Appender interface are not goroutine-safe.
type Appender interface {


	// Add adds a sample pair for the given series.
	//
	// A reference number is returned which can be used to add further samples in the same or later transactions.
	//
	// Returned reference numbers are ephemeral and may be rejected in calls to AddFast() at any point.
	//
	// Adding the sample via Add() returns a new reference number.
	//
	// If the reference is 0 it must not be used for caching.

	Add(l labels.Labels, t int64, v float64) (uint64, error)







	// AddFast adds a sample pair for the referenced series.
	//
	// It is generally faster than adding a sample by providing its full label set.
	//
	//
	AddFast(ref uint64, t int64, v float64) error




	// Commit submits the collected samples and purges the batch.
	//
	// If Commit returns a non-nil error,
	// it also rolls back all modifications made in the appender so far, as Rollback would do.
	//
	// In any case, an Appender must not be used anymore after Commit has been called.
	Commit() error



	// Rollback rolls back all modifications made in the appender so far.
	//
	// Appender has to be discarded after rollback.
	Rollback() error
}








// SeriesSet contains a set of series.
//
// 迭代器
type SeriesSet interface {

	// 读入一条时序数据，不存在返回 false
	Next() bool

	// 读取当前时序数据
	At() Series

	// 错误信息，当读取出错时，Next() 会返回 false，然后通过 Err() 判断是否出错
	Err() error
}


var emptySeriesSet = errSeriesSet{}

// EmptySeriesSet returns a series set that's always empty.
func EmptySeriesSet() SeriesSet {
	return emptySeriesSet
}

type errSeriesSet struct {
	err error
}

func (s errSeriesSet) Next() bool { return false }
func (s errSeriesSet) At() Series { return nil }
func (s errSeriesSet) Err() error { return s.err }







// Series exposes a single time series and allows iterating over samples.
type Series interface {

	// Labels returns the complete set of labels identifying the series.
	Labels

	// Iterator returns a new iterator of the data of the series.
	SampleIteratable
}



// ChunkSeriesSet contains a set of chunked series.
type ChunkSeriesSet interface {
	Next() bool
	At() ChunkSeries
	Err() error
}


// ChunkSeries exposes a single time series and allows iterating over chunks.
type ChunkSeries interface {

	// Labels.Labels() returns the complete set of labels.
	Labels

	// ChunkIterator.Iterator() returns a new iterator that iterates over non-overlapping chunks of the series.
	ChunkIteratable
}







// Labels represents an item that has labels e.g. time series.
//
// 在 prometheus 中，一个时序数据可由一组 Labels 唯一标识，所以 Series 接口内嵌了 Labels 接口。
//
type Labels interface {

	// Labels returns the complete set of labels.
	//
	// For series it means all labels identifying the series.
	Labels() labels.Labels
}


type SampleIteratable interface {

	// Iterator returns a new iterator of the data of the series.
	Iterator() chunkenc.Iterator

}

type ChunkIteratable interface {

	// ChunkIterator returns a new iterator that iterates over non-overlapping chunks of the series.
	Iterator() chunks.Iterator

}

// TODO(bwplotka): Remove in next Pr.
type DeprecatedChunkSeriesSet interface {
	Next() bool
	At() (labels.Labels, []chunks.Meta, tombstones.Intervals)
	Err() error
}

type Warnings []error
