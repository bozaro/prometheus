// Copyright 2015 The Prometheus Authors
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

package etcd

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"gopkg.in/yaml.v2"
)

var (
	// DefaultSDConfig is the default Etcd SD configuration.
	DefaultSDConfig = SDConfig{
		Endpoints: []string{"localhost:2379"},
		Prefix:    "metrics-registry/v1/",
	}
)

// SDConfig is the configuration for Etcd service discovery.
type SDConfig struct {
	Endpoints []string `yaml:"endpoint,omitempty"`
	Prefix    string   `yaml:"prefix,omitempty"`
}

type discoveryItem struct {
	Address string `yaml:"address"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *SDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultSDConfig
	type plain SDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	if len(c.Endpoints) == 0 {
		return fmt.Errorf("etcd SD configuration requires a non-empty endpoints")
	}
	return nil
}

// Discovery retrieves target information from a Consul server
// and updates them via watches.
type Discovery struct {
	client *clientv3.Client
	prefix string
	logger log.Logger
}

// NewDiscovery returns a new Discovery for the given config.
func NewDiscovery(conf *SDConfig, logger log.Logger) (*Discovery, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	etcdConf := clientv3.Config{
		Endpoints: conf.Endpoints,
	}
	client, err := clientv3.New(etcdConf)
	if err != nil {
		return nil, err
	}
	cd := &Discovery{
		client: client,
		prefix: conf.Prefix,
		logger: logger,
	}
	return cd, nil
}

// Run implements the Discoverer interface.
func (d *Discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	discovered := make(map[string]struct{})

	deleteKey := func(key string) {
		if _, ok := discovered[key]; ok {
			ch <- d.targetGroup(key, nil)
			delete(discovered, key)
		}
	}
	updateKey := func(key string, value []byte) {
		di := d.parseDiscoveryItem(key, value)
		if di == nil {
			deleteKey(key)
			return
		}
		discovered[key] = struct{}{}
		ch <- d.targetGroup(key, di)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		initialList, revision, err := etcdReadData(ctx, d.client, d.prefix)
		if err != nil {
			level.Error(d.logger).Log("msg", "Error reading initial list from etcd", "err", err)
			continue
		}
		// Update old servers list with new data
		for key := range discovered {
			if _, ok := initialList[key]; !ok {
				deleteKey(key)
			}
		}
		for key, value := range initialList {
			updateKey(key, value)
		}
		// Listen events
		watchChan := d.client.Watch(ctx, d.prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
		for resp := range watchChan {
			for _, evt := range resp.Events {
				key := string(evt.Kv.Key)
				switch evt.Type {
				case mvccpb.PUT:
					updateKey(key, evt.Kv.Value)
				case mvccpb.DELETE:
					deleteKey(key)
				}
			}
		}
	}
}

func (d *Discovery) parseDiscoveryItem(key string, data []byte) *discoveryItem {
	var result discoveryItem
	if err := yaml.UnmarshalStrict(data, &result); err != nil {
		level.Error(d.logger).Log("msg", "Error parsing etcd discovery entry", "err", err, "key", key, "value", string(data))
		return nil
	}
	return &result
}

func (d *Discovery) targetGroup(key string, di *discoveryItem) []*targetgroup.Group {
	group := &targetgroup.Group{
		Source: key[len(d.prefix):],
	}
	if di != nil {
		labels := model.LabelSet{
			model.AddressLabel: model.LabelValue(di.Address),
		}
		group.Targets = []model.LabelSet{labels}
	}
	return []*targetgroup.Group{group}
}

// Read snapshot of etcd data
func etcdReadData(ctx context.Context, etcdClient clientv3.KV, prefix string) (map[string][]byte, int64, error) {
	rangeBeg := prefix
	rangeEnd := clientv3.GetPrefixRangeEnd(rangeBeg)

	result := make(map[string][]byte)
	var revision int64
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(rangeEnd)}
		if revision >= 0 {
			opts = append(opts, clientv3.WithRev(revision))
		}
		resp, err := etcdClient.Get(ctx, rangeBeg, opts...)
		if err != nil {
			return nil, 0, fmt.Errorf("error reading etcd values: %v", err)
		}

		if revision < 0 {
			revision = resp.Header.Revision
		}

		for _, kv := range resp.Kvs {
			result[string(kv.Key)] = kv.Value
		}

		if !resp.More {
			return result, revision, nil
		}
		lastKey := resp.Kvs[len(resp.Kvs)-1].Key
		rangeEnd = string(append(append(make([]byte, 0, len(lastKey)+1), lastKey...), 0))
	}
}
