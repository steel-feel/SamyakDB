package discovery

import (
	"fmt"
	"net"
	"strings"

	"github.com/hashicorp/memberlist"
)

type Membership struct {
	Config Config
	handler Handler
	list   *memberlist.Memberlist
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

type Handler interface {
	Join(name, addr string, tags map[string]string) error
	Leave(name string) error
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
	}
	if err := c.setupMemberlist(); err != nil {
		return nil, err
	}
	return c, nil
}

func (m *Membership) setupMemberlist() error {
	conf := memberlist.DefaultLANConfig()
	conf.Name = m.Config.NodeName
	
	host, portStr, err := net.SplitHostPort(m.Config.BindAddr)
	if err != nil {
		return err
	}
	
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	conf.BindPort = port

	if host != "" {
		conf.BindAddr = host
		conf.AdvertiseAddr = host
	}
	conf.AdvertisePort = port
	
	conf.Events = &eventHandler{
		handler: m.handler,
	}
	conf.Delegate = &delegate{
		tags: m.Config.Tags,
	}
	
	list, err := memberlist.Create(conf)
	if err != nil {
		return err
	}
	m.list = list
	if len(m.Config.StartJoinAddrs) > 0 {
		_, err = list.Join(m.Config.StartJoinAddrs)
		if err != nil {
			return err
		}
	}
	return nil
}

type delegate struct {
	tags map[string]string
}

func (d *delegate) NodeMeta(limit int) []byte {
	// Simple comma-separated key=value pairs for metadata
	var meta string
	for k, v := range d.tags {
		if meta != "" {
			meta += ","
		}
		meta += fmt.Sprintf("%s=%s", k, v)
	}
	return []byte(meta)
}

func (d *delegate) NotifyMsg([]byte) {}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (d *delegate) LocalState(join bool) []byte {
	return nil
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {}

type eventHandler struct {
	handler Handler
}

func decodeMeta(meta []byte) map[string]string {
	tags := make(map[string]string)
	if len(meta) == 0 {
		return tags
	}
	pairs := strings.Split(string(meta), ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) == 2 {
			tags[kv[0]] = kv[1]
		}
	}
	return tags
}

func (e *eventHandler) NotifyJoin(node *memberlist.Node) {
	if e.handler != nil {
		tags := decodeMeta(node.Meta)
		e.handler.Join(node.Name, node.Address(), tags)
	}
}

func (e *eventHandler) NotifyLeave(node *memberlist.Node) {
	if e.handler != nil {
		e.handler.Leave(node.Name)
	}
}

func (e *eventHandler) NotifyUpdate(node *memberlist.Node) {}

func (m *Membership) Members() []memberlist.Node {
	nodes := m.list.Members()
	res := make([]memberlist.Node, len(nodes))
	for i, n := range nodes {
		res[i] = *n
	}
	return res
}

func (m *Membership) Close() error {
	return m.list.Leave(0)
}
