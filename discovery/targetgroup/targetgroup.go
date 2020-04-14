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

package targetgroup

import (
	"bytes"
	"encoding/json"

	"github.com/prometheus/common/model"
)

// Group is a set of targets with a common label set(production, test, staging etc.).
//
// Group 是一组具有公共标签集的 targets，公共标签集如（region, env...)
//
type Group struct {

	// Targets is a list of targets identified by a label set.
	// Each target is uniquely identifiable in the group by its address label.
	//
	// Targets 代表来一组 targets
	//
	// 每个 target 是一个 model.LabelSet 对象，这个 model.LabelSet 对象可以唯一代表该 target 。
	//
	// 这些 targets 以其 address label 作为在 Group 内的唯一标识，例："__address__": "localhost:9100"。
	//
	Targets []model.LabelSet

	// Labels is a set of labels that is common across all targets in the group.
	Labels model.LabelSet

	// Source is an identifier that describes a group of targets.
	Source string 	// 全局唯一ID，示例：Source: "0"



}

func (tg Group) String() string {
	return tg.Source
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (tg *Group) UnmarshalYAML(unmarshal func(interface{}) error) error {
	g := struct {
		Targets []string       `yaml:"targets"`
		Labels  model.LabelSet `yaml:"labels"`
	}{}
	if err := unmarshal(&g); err != nil {
		return err
	}
	tg.Targets = make([]model.LabelSet, 0, len(g.Targets))
	for _, t := range g.Targets {
		tg.Targets = append(tg.Targets, model.LabelSet{
			model.AddressLabel: model.LabelValue(t),
		})
	}
	tg.Labels = g.Labels
	return nil
}

// MarshalYAML implements the yaml.Marshaler interface.
func (tg Group) MarshalYAML() (interface{}, error) {
	g := &struct {
		Targets []string       `yaml:"targets"`
		Labels  model.LabelSet `yaml:"labels,omitempty"`
	}{
		Targets: make([]string, 0, len(tg.Targets)),
		Labels:  tg.Labels,
	}
	for _, t := range tg.Targets {
		g.Targets = append(g.Targets, string(t[model.AddressLabel]))
	}
	return g, nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (tg *Group) UnmarshalJSON(b []byte) error {
	g := struct {
		Targets []string       `json:"targets"`
		Labels  model.LabelSet `json:"labels"`
	}{}

	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&g); err != nil {
		return err
	}
	tg.Targets = make([]model.LabelSet, 0, len(g.Targets))
	for _, t := range g.Targets {
		tg.Targets = append(tg.Targets, model.LabelSet{
			model.AddressLabel: model.LabelValue(t),
		})
	}
	tg.Labels = g.Labels
	return nil
}
