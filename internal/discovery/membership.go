package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Config struct {
	// NodeName is the name of the node in the Serf cluster. It should be unique across the cluster.
	NodeName string
	// BindAddr is the address that Serf will bind to for cluster communication.
	// It should be in the form "host:port".
	BindAddr string
	// Tags are the key-value pairs metadata associated with this node and shared with other nodes in the cluster.
	Tags map[string]string
	// StartJoinAddrs is a list of addresses of existing cluster members to join on startup.
	StartJoinAddrs []string
}

// Handler defines the interface for handling cluster membership events.
// The Join and Leave methods are called when nodes join or leave the cluster, respectively.
type Handler interface {
	// Join is called when a new node joins the cluster.
	Join(name, addr string) error
	// Leave is called when a node leaves the cluster.
	Leave(name string) error
}

type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}
	logger.Named("membership")

	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  logger,
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()

	// define where Serf will bind to for cluster communication
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	// define the event channel
	m.events = make(chan serf.Event)
	config.EventCh = m.events

	// define the node name and tags
	config.NodeName = m.NodeName
	config.Tags = m.Tags

	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()

	// join the cluster if there are any existing members
	if m.StartJoinAddrs != nil {
		_, err := m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			// type assertion never fails because only MemberEvent could have this event type
			me := e.(serf.MemberEvent)
			for _, member := range me.Members {
				// gossip protocol will propagate the message to all nodes include the local node
				// so we need to ignore the event if it's about the local node
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave:
			// type assertion never fails because only MemberEvent could have this event type
			me := e.(serf.MemberEvent)
			for _, member := range me.Members {
				// gossip protocol will propagate the message to all nodes include the local node
				// so we need to ignore the event if it's about the local node
				if m.isLocal(member) {
					continue
				}
				m.handleLeave(member)
			}
		default:
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		// log the error but don't return it because we don't want to stop handling other events
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		// log the error but don't return it because we don't want to stop handling other events
		m.logError(err, "failed to leave", member)
	}
}

// isLocal checks if the given member is the local member.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// membersAlive counts the number of alive members in the cluster.
func (m *Membership) membersAlive() int {
	count := 0
	for _, m := range m.serf.Members() {
		if m.Status == serf.StatusAlive {
			count++
		}
	}
	return count
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave makes the node leave the Serf cluster gracefully.
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
