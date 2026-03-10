package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

// handler tracks the join and leave events for testing purposes.
// if handler is not initialized with channels, it will ignore the events.
type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(name, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"name": name,
			"addr": addr,
		}
	}
	return nil
}

func (h *handler) Leave(name string) error {
	if h.leaves != nil {
		h.leaves <- name
	}
	return nil
}

// setupMember creates a new Membership instance for testing. It takes the existing members to join
// and returns the updated list of members and the handler for event tracking.
func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_addr": addr,
	}
	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	h := &handler{}

	// only the first member will have the channels initialized to track events, the rest will ignore them.
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		c.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}
	m, err := New(h, c)
	require.NoError(t, err)
	members = append(members, m)
	return members, h
}

func TestMembership(t *testing.T) {
	m, h := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		// expect 2 join events for the 2 new members
		assert.Equal(c, 2, len(h.joins), "expected 2 join events")

		for _, m := range m {
			assert.Equal(c, 3, len(m.serf.Members()), "expected all nodes to have 3 members")
		}

	}, time.Second*5, time.Millisecond*100, "expected all nodes joined the cluster")

	// leave the last node and expect a leave event
	require.NoError(t, m[2].serf.Leave())

	// the remaining nodes should have 2 alive members
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, len(h.leaves), "expected 1 leave event")
		// Notes: member leaves are not immediately reflected in the member list
		for _, m := range m[:2] {
			assert.Equal(c, 2, m.membersAlive(), "expected remaining nodes to have 2 alive members")
		}
	}, time.Second*5, time.Millisecond*100, "expected remaining nodes to have 2 alive members")
}
